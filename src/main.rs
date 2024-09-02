use futures::StreamExt;
use libp2p::{
    core::{upgrade, transport::Transport},
    gossipsub,
    identity,
    mdns,
    noise,
    swarm::{SwarmEvent, Swarm, Config},
    tcp,
    Multiaddr,
    PeerId,
};
use libp2p_swarm_derive::NetworkBehaviour;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{info, warn, error, debug, instrument};
use serde_json::json;
use jsonrpc_core::{IoHandler, Params, Value, Error as JsonRpcError};
use jsonrpc_http_server::ServerBuilder;
use serde_json::Value as JsonValue;

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "NodeBehaviourEvent")]
struct NodeBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[derive(Debug)]
enum NodeBehaviourEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<gossipsub::Event> for NodeBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        NodeBehaviourEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for NodeBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        NodeBehaviourEvent::Mdns(event)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DataWithClock {
    data: String,
    #[serde(with = "crate::vector_clock_serde")]
    vector_clock: HashMap<PeerId, u64>,
}

mod vector_clock_serde {
    use super::*;
    use serde::{Serializer, Deserializer};
    use std::str::FromStr;

    pub fn serialize<S>(
        vector_clock: &HashMap<PeerId, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(vector_clock.len()))?;
        for (k, v) in vector_clock {
            map.serialize_entry(&k.to_base58(), v)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<PeerId, u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string_map: HashMap<String, u64> = HashMap::deserialize(deserializer)?;
        string_map
            .into_iter()
            .map(|(k, v)| {
                Ok((
                    PeerId::from_str(&k).map_err(serde::de::Error::custom)?,
                    v,
                ))
            })
            .collect()
    }
}

struct Node {
    swarm: Swarm<NodeBehaviour>,
    vector_clock: HashMap<PeerId, u64>,
    topic: gossipsub::IdentTopic,
}

impl Node {
    fn new(swarm: Swarm<NodeBehaviour>) -> Self {
        let topic = gossipsub::IdentTopic::new("relay_data");
        Node {
            swarm,
            vector_clock: HashMap::new(),
            topic,
        }
    }

    #[instrument(skip(self, shutdown, rx))]
    async fn start(&mut self, shutdown: Arc<Notify>, mut rx: mpsc::Receiver<String>) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    if let Err(e) = self.handle_swarm_event(event).await {
                        error!(error = ?e, "Error handling swarm event");
                    }
                }
                Some(message) = rx.recv() => {
                    if let Err(e) = self.handle_message(message).await {
                        error!(error = ?e, "Error handling message");
                    }
                }
                _ = shutdown.notified() => {
                    info!("Received shutdown signal");
                    break;
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, event), fields(event_type = ?event))]
    async fn handle_swarm_event(&mut self, event: SwarmEvent<NodeBehaviourEvent>) -> Result<(), Box<dyn Error + Send + Sync>> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!(?address, "Listening on new address");
            }
            SwarmEvent::Behaviour(NodeBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                                                                    propagation_source,
                                                                    message_id,
                                                                    message,
                                                                })) => {
                debug!(
                    message = %String::from_utf8_lossy(&message.data),
                    ?message_id,
                    ?propagation_source,
                    "Received gossipsub message"
                );
                match serde_json::from_slice::<DataWithClock>(&message.data) {
                    Ok(received_data) => {
                        if let Err(e) = self.process_received_data(received_data).await {
                            warn!(error = ?e, "Error processing received data");
                        }
                    }
                    Err(e) => {
                        warn!(error = ?e, "Failed to deserialize received data");
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    #[instrument(skip(self, received_data), fields(data_len = received_data.data.len()))]
    async fn process_received_data(&mut self, mut received_data: DataWithClock) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (peer, &clock) in &received_data.vector_clock {
            self.vector_clock
                .entry(*peer)
                .and_modify(|e| *e = std::cmp::max(*e, clock))
                .or_insert(clock);
        }
        let local_peer_id = *self.swarm.local_peer_id();
        *self.vector_clock.entry(local_peer_id).or_insert(0) += 1;

        received_data.vector_clock = self.vector_clock.clone();

        debug!(received_data = ?received_data, "Processed data");

        self.publish_data_with_clock(received_data).await
    }

    #[instrument(skip(self))]
    async fn publish_data(&mut self, data: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let local_peer_id = *self.swarm.local_peer_id();
        *self.vector_clock.entry(local_peer_id).or_insert(0) += 1;

        let data_with_clock = DataWithClock {
            data,
            vector_clock: self.vector_clock.clone(),
        };

        self.publish_data_with_clock(data_with_clock).await
    }

    #[instrument(skip(self, data_with_clock), fields(data_len = data_with_clock.data.len()))]
    async fn publish_data_with_clock(&mut self, data_with_clock: DataWithClock) -> Result<(), Box<dyn Error + Send + Sync>> {
        match serde_json::to_string(&data_with_clock) {
            Ok(data) => {
                self.swarm.behaviour_mut().gossipsub.publish(self.topic.clone(), data.as_bytes())
                    .map_err(|e| {
                        error!(error = ?e, "Failed to publish data");
                        Box::<dyn Error + Send + Sync>::from(e)
                    })?;
                info!("Data published successfully");
            }
            Err(e) => {
                error!(error = ?e, "Failed to serialize data");
                return Err(Box::new(e));
            }
        }
        Ok(())
    }

    async fn connect_to_peer(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.swarm.dial(addr.clone())?;
        info!("Dialed {}", addr);
        Ok(())
    }

    async fn handle_message(&mut self, message: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(addr) = message.strip_prefix("connect ") {
            let addr: Multiaddr = addr.parse()?;
            self.connect_to_peer(addr).await?;
        } else {
            self.publish_data(message).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct NodeService {
    tx: mpsc::Sender<String>,
}

impl NodeService {
    fn new(tx: mpsc::Sender<String>) -> Self {
        NodeService { tx }
    }

    async fn submit_data(&self, params: Params) -> Result<Value, JsonRpcError> {
        let params: Vec<JsonValue> = params.parse().map_err(|_| JsonRpcError::invalid_params("Invalid parameters"))?;
        let data = params.get(0)
            .and_then(|v| v.as_str())
            .ok_or_else(|| JsonRpcError::invalid_params("Expected a single string parameter"))?;

        self.tx.send(data.to_string()).await.map_err(|e| {
            error!(error = ?e, "Failed to send data through channel");
            JsonRpcError::internal_error()
        })?;
        info!("Data submitted successfully");
        Ok(json!("Data submitted successfully"))
    }
}

async fn create_node(port: u16) -> Result<(Node, mpsc::Sender<String>, PeerId), Box<dyn Error + Send + Sync>> {
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    info!(?peer_id, "Created peer");

    let transport = tcp::tokio::Transport::default()
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(libp2p::yamux::Config::default())
        .boxed();

    let behaviour = {
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .expect("Valid config");
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(id_keys),
            gossipsub_config
        ).expect("Valid configuration");

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
            .expect("Valid config");

        NodeBehaviour { gossipsub, mdns }
    };

    let config = Config::with_tokio_executor();
    let mut swarm = Swarm::new(transport, behaviour, peer_id, config);

    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", port).parse()?)?;

    let (tx, rx) = mpsc::channel::<String>(32);
    let node = Node::new(swarm);

    Ok((node, tx, peer_id))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let shutdown = Arc::new(Notify::new());
    let nodes = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    // Create 4 nodes
    for port in 30000..30004 {
        let (node, tx, peer_id) = create_node(port).await?;
        let node_service = NodeService::new(tx.clone());

        let mut io = IoHandler::new();
        let node_service_clone = node_service.clone();
        io.add_method("submit_data", move |params: Params| {
            let service = node_service_clone.clone();
            async move {
                service.submit_data(params).await
            }
        });

        let rpc_port = port + 1;
        let server = ServerBuilder::new(io)
            .threads(3)
            .start_http(&format!("127.0.0.1:{}", rpc_port).parse().unwrap())
            .expect("Unable to start RPC server");

        info!("JSON-RPC server for peer {:?} listening on http://127.0.0.1:{}", peer_id, rpc_port);

        let shutdown_clone = shutdown.clone();
        let nodes_clone = nodes.clone();
        let (tx_node, rx_node) = mpsc::channel::<String>(32);
        let handle = tokio::spawn(async move {
            let mut node = node;
            if let Err(e) = node.start(shutdown_clone, rx_node).await {
                error!(error = ?e, "Node error");
            }
            let mut nodes = nodes_clone.lock().await;
            if let Some(pos) = nodes.iter().position(|&(p, _)| p == peer_id) {
                nodes.remove(pos);
            }
        });

        handles.push(handle);
        nodes.lock().await.push((peer_id, tx_node));
    }

    // Connect nodes to each other
    let nodes_vec = nodes.lock().await.iter().cloned().collect::<Vec<_>>();
    for (i, (peer_id_i, _)) in nodes_vec.iter().enumerate() {
        for (j, (_, _)) in nodes_vec.iter().enumerate() {
            if i != j {
                let addr = format!("/ip4/127.0.0.1/tcp/{}", 30000 + j);
                if let Some((_, tx)) = nodes.lock().await.iter().find(|(p, _)| p == peer_id_i) {
                    tx.send(format!("connect {}", addr)).await.map_err(|e| Box::<dyn Error + Send + Sync>::from(e))?;
                }
            }
        }
    }

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
    info!("Received Ctrl+C, initiating shutdown");
    shutdown.notify_waiters();

    // Wait for all node tasks to finish
    for handle in handles {
        handle.await?;
    }

    info!("All nodes stopped, exiting");
    Ok(())
}