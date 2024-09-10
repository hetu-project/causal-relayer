use crate::behaviour::{NodeBehaviour, NodeEvent};
use base64::{engine::general_purpose, Engine as _};
use config::Config;
use jsonrpc_core::futures_util::StreamExt;
use libp2p::core::upgrade;
use libp2p::{
    gossipsub, identity,
    swarm::{Swarm, SwarmEvent},
    Multiaddr, PeerId, Transport,
};
use libp2p::{noise, tcp, yamux};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task;
use tokio::time::{interval, timeout};
use tracing::{error, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataWithClock {
    pub data: String,
    #[serde(with = "peer_id_serde")]
    pub vector_clock: HashMap<PeerId, u64>,
    pub timestamp: u64,
}

impl PartialOrd for DataWithClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataWithClock {
    fn cmp(&self, other: &Self) -> Ordering {
        // First, compare vector clocks
        for (peer, &self_count) in &self.vector_clock {
            match other.vector_clock.get(peer) {
                Some(&other_count) => {
                    if self_count != other_count {
                        return self_count.cmp(&other_count);
                    }
                }
                None => return Ordering::Greater,
            }
        }

        for peer in other.vector_clock.keys() {
            if !self.vector_clock.contains_key(peer) {
                return Ordering::Less;
            }
        }

        // If vector clocks are equal, compare timestamps
        self.timestamp.cmp(&other.timestamp)
    }
}

mod peer_id_serde {
    use libp2p::PeerId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;
    use std::str::FromStr;

    pub fn serialize<S>(
        vector_clock: &HashMap<PeerId, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stringified: HashMap<String, u64> = vector_clock
            .iter()
            .map(|(k, v)| (k.to_base58(), *v))
            .collect();
        stringified.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<PeerId, u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stringified: HashMap<String, u64> = HashMap::deserialize(deserializer)?;
        stringified
            .into_iter()
            .map(|(k, v)| Ok((PeerId::from_str(&k).map_err(serde::de::Error::custom)?, v)))
            .collect()
    }
}

pub struct NodeInner {
    swarm: Swarm<NodeBehaviour>,
    vector_clock: HashMap<PeerId, u64>,
    topic: gossipsub::IdentTopic,
    stored_data: BTreeMap<DataWithClock, ()>,
    tombstones: HashMap<String, Tombstone>,
}

pub struct Node {
    inner: Arc<Mutex<NodeInner>>,
    drain_sender: mpsc::Sender<oneshot::Sender<DrainResult>>,
}

type DrainResult = Result<Vec<DataWithClock>, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tombstone {
    pub data_hash: String,
    pub drained_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    Data(DataWithClock),
    Tombstone(Tombstone),
}

impl Node {
    pub async fn new(swarm: Swarm<NodeBehaviour>) -> Self {
        let (drain_sender, mut drain_receiver) = mpsc::channel::<oneshot::Sender<DrainResult>>(100);
        let inner = Arc::new(Mutex::new(NodeInner {
            swarm,
            vector_clock: HashMap::new(),
            topic: gossipsub::IdentTopic::new("relay_data"),
            stored_data: BTreeMap::new(),
            tombstones: Default::default(),
        }));

        let inner_clone = inner.clone();
        task::spawn(async move {
            while let Some(response_sender) = drain_receiver.recv().await {
                let result = Self::handle_drain(&inner_clone).await;
                let _ = response_sender.send(result);
            }
        });

        Node { inner, drain_sender }
    }

    pub async fn drain_data(&self) -> DrainResult {
        let (response_sender, response_receiver) = oneshot::channel();
        self.drain_sender.send(response_sender).await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        response_receiver.await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?
    }

    async fn handle_drain(inner: &Arc<Mutex<NodeInner>>) -> DrainResult {
        let mut inner = inner.lock().await;
        let drained_data = std::mem::take(&mut inner.stored_data).into_keys().collect::<Vec<_>>();
        let topic = inner.topic.clone();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        for data in &drained_data {
            let data_hash = calculate_hash(data);
            let tombstone = Tombstone {
                data_hash: data_hash.clone(),
                drained_at: now,
            };

            inner.tombstones.insert(data_hash, tombstone.clone());

            let sync_message = SyncMessage::Tombstone(tombstone);
            let serialized_message = serde_json::to_string(&sync_message)?;

            if let Err(e) = inner
                .swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), serialized_message.into_bytes())
            {
                error!("Failed to publish tombstone: {:?}", e);
            }
        }

        Ok(drained_data)
    }

    pub async fn create(config: &Config) -> Result<(Self, PeerId), Box<dyn Error + Send + Sync>> {
        let private_key_str = config.get_string("node.private_key")?;
        let id_keys = if private_key_str.is_empty() {
            let keys = identity::Keypair::generate_ed25519();
            let encoded_private_key =
                general_purpose::STANDARD.encode(keys.to_protobuf_encoding()?);
            info!("Generated new private key. Add this to your config to reuse the same peer ID:");
            info!("private_key = \"{}\"", encoded_private_key);
            keys
        } else {
            let decoded_key = general_purpose::STANDARD.decode(private_key_str)?;
            identity::Keypair::from_protobuf_encoding(&decoded_key)?
        };

        let peer_id = PeerId::from(id_keys.public());
        info!(?peer_id, "Using peer ID");

        let transport = tcp::tokio::Transport::default()
            .upgrade(upgrade::Version::V1)
            .authenticate(
                noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"),
            )
            .multiplex(yamux::Config::default())
            .boxed();

        let behaviour = NodeBehaviour::new(&id_keys, peer_id);

        let swarm_config = libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(3000));
        let mut swarm = Swarm::new(transport, behaviour, peer_id, swarm_config);
        let p2p_port = config.get_int("network.p2p_port")? as u16;
        swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", p2p_port).parse()?)?;

        let node = Node::new(swarm).await;
        node.subscribe_to_topic().await?;

        Ok((node, peer_id))
    }

    async fn process_received_data(
        &self,
        mut data: DataWithClock,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.lock().await;
        let data_hash = calculate_hash(&data);

        if inner.tombstones.contains_key(&data_hash) {
            info!("Received data has been drained, ignoring: {:?}", data);
            return Ok(());
        }

        let local_peer_id = *inner.swarm.local_peer_id();
        let should_update_clock = !data.vector_clock.contains_key(&local_peer_id);

        if !inner.stored_data.contains_key(&data) || should_update_clock {
            if should_update_clock {
                // Increment local clock
                let local_clock = inner.vector_clock.entry(local_peer_id).or_insert(0);
                *local_clock += 1;

                // Update the data's vector clock with our local clock
                data.vector_clock.insert(local_peer_id, *local_clock);

                // Update timestamp
                data.timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs();
            }

            //TODO: remove duplicate data
            inner.stored_data.retain(|k, _| k.data != data.data);
            inner.stored_data.insert(data.clone(), ());
            // info!("Stored new or updated data: {:?}", data);

            // Republish the data with updated clock
            let sync_message = SyncMessage::Data(data);
            let topic = inner.topic.clone();
            let serialized_message = serde_json::to_string(&sync_message)?;
            inner
                .swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic, serialized_message.into_bytes())?;
        } else {
            info!("Received data already exists and doesn't need updating.");
        }

        Ok(())
    }

    async fn process_tombstones(
        &self,
        tombstones: Vec<Tombstone>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.lock().await;
        let mut new_tombstones = Vec::new();

        // Collect all new tombstones
        for tombstone in tombstones {
            if !inner.tombstones.contains_key(&tombstone.data_hash) {
                new_tombstones.push(tombstone);
            }
        }

        if !new_tombstones.is_empty() {
            // Clear all stored data
            let cleared_data = std::mem::take(&mut inner.stored_data);

            // Create a HashSet of tombstone hashes for faster lookup
            let tombstone_hashes: std::collections::HashSet<_> =
                new_tombstones.iter().map(|t| t.data_hash.clone()).collect();

            // Re-add data that isn't contained in any of the new tombstones
            for (data, _) in cleared_data {
                if !tombstone_hashes.contains(&calculate_hash(&data)) {
                    inner.stored_data.insert(data, ());
                }
            }

            // Add the new tombstones
            for tombstone in &new_tombstones {
                inner
                    .tombstones
                    .insert(tombstone.data_hash.clone(), tombstone.clone());
            }

            // Republish the tombstones
            let topic = inner.topic.clone();
            for tombstone in new_tombstones {
                let sync_message = SyncMessage::Tombstone(tombstone);
                let serialized_message = serde_json::to_string(&sync_message)?;
                inner
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), serialized_message.into_bytes())?;
            }
        }

        Ok(())
    }


    pub async fn subscribe_to_topic(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.lock().await;
        let topic = inner.topic.clone();
        if let Err(e) = inner.swarm.behaviour_mut().gossipsub.subscribe(&topic) {
            error!("Failed to subscribe to topic: {:?}", e);
            Err(Box::new(e))
        } else {
            info!("Subscribed to topic: {:?}", topic);
            Ok(())
        }
    }

    pub async fn connect_to_bootstrap_peers(
        &self,
        config: &Config,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.lock().await;
        if let Ok(bootstrap_peers) = config.get_array("node.bootstrap_peers") {
            for peer in bootstrap_peers {
                if let Ok(addr) = peer.into_string() {
                    match addr.parse::<Multiaddr>() {
                        Ok(multiaddr) => {
                            info!("Connecting to bootstrap peer: {}", addr);
                            match inner.swarm.dial(multiaddr.clone()) {
                                Ok(_) => info!("Dialing peer: {}", addr),
                                Err(e) => warn!("Failed to dial peer {}: {:?}", addr, e),
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse bootstrap peer address {}: {:?}", addr, e);
                        }
                    }
                }
            }
        } else {
            info!("No bootstrap peers configured");
        }
        Ok(())
    }

    pub async fn print_node_addresses(
        &self,
        config: &Config,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let inner = self.inner.lock().await;
        let local_peer_id = *inner.swarm.local_peer_id();
        let listened_addrs = inner.swarm.listeners().cloned().collect::<Vec<_>>();
        let p2p_port = config.get_int("network.p2p_port")? as u16;
        let external_ip = config.get_string("network.external_ip")?;

        if listened_addrs.is_empty() {
            warn!("No external addresses found. The node might not be publicly accessible.");
        } else {
            info!("Node addresses:");
            for addr in listened_addrs {
                info!("{}/p2p/{}", addr, local_peer_id);
            }
        }

        info!("External IP address (from config): {}", external_ip);
        info!(
            "Full address with external IP: /ip4/{}/tcp/{}/p2p/{}",
            external_ip, p2p_port, local_peer_id
        );

        info!("You can share these addresses with others to allow them to connect to your node.");
        Ok(())
    }

    pub async fn run(
        &self,
        mut rx: mpsc::Receiver<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut log_interval = interval(Duration::from_secs(5));
        let mut tombstone_batch = Vec::new();
        let mut tombstone_process_interval = interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = log_interval.tick() => {
                    let mut inner = self.inner.lock().await;
                    let connected_peers: Vec<_> = inner.swarm.connected_peers().collect();
                    info!("Connected peers: {} - {:?}", connected_peers.len(), connected_peers);

                    // Log GossipSub information
                    let gossipsub = &inner.swarm.behaviour().gossipsub;
                    let topic_peers = gossipsub.topics().fold(0, |acc, topic| acc + gossipsub.mesh_peers(topic).count());
                    let all_peers = gossipsub.all_peers().count();
                    info!("GossipSub info:");
                    info!("  Topics: {:?}", gossipsub.topics().collect::<Vec<_>>());
                    info!("  Peers in topics: {}", topic_peers);
                    info!("  All known peers: {}", all_peers);
                    info!("  Vector clock: {:?}", inner.vector_clock);
                    let sorted_data = inner.stored_data.keys().cloned().collect::<Vec<_>>();
                    info!("Stored data (sorted by vector clock):");
                    for data in sorted_data {
                        info!("  Vector Clock: {:?}, Timestamp: {}, Data: {}", data.vector_clock, data.timestamp, data.data);
                    }
                }

                Some(message) = rx.recv() => {
                    let mut inner = self.inner.lock().await;
                    info!("Received data from RPC: {}", message);
                    let local_peer_id = *inner.swarm.local_peer_id();
                    let local_clock = inner.vector_clock.entry(local_peer_id).or_insert(0);
                    *local_clock += 1;

                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs();

                    let data_with_clock = DataWithClock {
                        data: message,
                        vector_clock: inner.vector_clock.clone(),
                        timestamp,
                    };

                    // Store the data locally
                    inner.stored_data.insert(data_with_clock.clone(), ());

                    info!("Storing and publishing data: {:?}", data_with_clock);

                    // Clone the topic before borrowing inner.swarm mutably
                    let topic = inner.topic.clone();
                    let sync_message = SyncMessage::Data(data_with_clock);
                    let serialized_message = serde_json::to_string(&sync_message)?;

                    if let Err(e) = inner.swarm.behaviour_mut().gossipsub.publish(topic, serialized_message.into_bytes()) {
                        error!("Failed to publish data: {:?}", e);
                    }
                }

                _ = tombstone_process_interval.tick() => {
                if !tombstone_batch.is_empty() {
                    if let Err(e) = self.process_tombstones(std::mem::take(&mut tombstone_batch)).await {
                        error!("Error processing tombstone batch: {:?}", e);
                    }
                }
            }


                event = self.next_event() => {
                    match event {
                        SwarmEvent::Behaviour(NodeEvent::Gossipsub(gossipsub::Event::Message {
                            propagation_source,
                            message_id,
                            message,
                        })) => {
                            match serde_json::from_slice::<SyncMessage>(&message.data) {
                                Ok(sync_message) => {
                                    match sync_message {
                                        SyncMessage::Data(data) => {
                                        if let Err(e) = self.process_received_data(data).await {
                                            error!("Error processing received data: {:?}", e);}
                                        }
                                        SyncMessage::Tombstone(tombstone) => {
                                        tombstone_batch.push(tombstone);
                                        }
                                    }
                                }

                                Err(e) => {
                                    error!("Failed to deserialize message: {:?}", e);
                                }
                            }
                        }
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!("Local node is listening on {:?}", address);
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            info!("Connected to peer: {:?}", peer_id);
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            info!("Disconnected from peer: {:?}", peer_id);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn next_event(&self) -> SwarmEvent<NodeEvent> {
        self.inner.lock().await.swarm.select_next_some().await
    }
}

fn calculate_hash(data: &DataWithClock) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data.data.as_bytes());
    format!("{:x}", hasher.finalize())
}