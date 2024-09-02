use futures::StreamExt;
use jsonrpc_core::{IoHandler, Params, Value};
use jsonrpc_http_server::ServerBuilder;
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
use std::time::Duration;
use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::signal;

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

// Custom serialization for PeerId
mod peer_id_serde {
    use super::*;
    use serde::{Serializer, Deserializer};
    use std::str::FromStr;

    pub fn serialize<S>(peer_id: &PeerId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&peer_id.to_base58())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let peer_id_str = String::deserialize(deserializer)?;
        PeerId::from_str(&peer_id_str).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DataWithClock {
    data: String,
    #[serde(with = "crate::vector_clock_serde")]
    vector_clock: HashMap<PeerId, u64>,
}

// New module for custom serialization of HashMap<PeerId, u64>
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

enum SwarmCommand {
    Publish(String),
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

    async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = mpsc::channel::<String>(32);
        let tx_clone = tx.clone();

        let mut io = IoHandler::default();
        io.add_method("submit_data", move |params: Params| {
            let tx = tx_clone.clone();
            async move {
                let data: String = params.parse().unwrap();
                tx.send(data).await.unwrap();
                Ok(Value::String("Data submitted successfully".into()))
            }
        });

        let server = ServerBuilder::new(io)
            .threads(3)
            .start_http(&"127.0.0.1:30001".parse().unwrap())
            .expect("Unable to start RPC server");

        println!("RPC server listening on port 30001");

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await?;
                }
                Some(data) = rx.recv() => {
                    self.publish_data(data).await?;
                }
                _ = signal::ctrl_c() => {
                    println!("Shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<NodeBehaviourEvent>) -> Result<(), Box<dyn Error>> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Listening on {:?}", address);
            }
            SwarmEvent::Behaviour(NodeBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                                                                    propagation_source,
                                                                    message_id,
                                                                    message,
                                                                })) => {
                println!(
                    "Got message: '{}' with id: {}, from peer: {:?}",
                    String::from_utf8_lossy(&message.data),
                    message_id,
                    propagation_source
                );
                if let Ok(received_data) = serde_json::from_slice::<DataWithClock>(&message.data) {
                    self.process_received_data(received_data).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn process_received_data(&mut self, mut received_data: DataWithClock) -> Result<(), Box<dyn Error>> {
        for (peer, &clock) in &received_data.vector_clock {
            self.vector_clock
                .entry(*peer)
                .and_modify(|e| *e = std::cmp::max(*e, clock))
                .or_insert(clock);
        }
        let local_peer_id = *self.swarm.local_peer_id();
        *self.vector_clock.entry(local_peer_id).or_insert(0) += 1;

        received_data.vector_clock = self.vector_clock.clone();

        println!("Processed data: {:?}", received_data);

        self.publish_data_with_clock(received_data).await
    }

    async fn publish_data(&mut self, data: String) -> Result<(), Box<dyn Error>> {
        let local_peer_id = *self.swarm.local_peer_id();
        *self.vector_clock.entry(local_peer_id).or_insert(0) += 1;

        let data_with_clock = DataWithClock {
            data,
            vector_clock: self.vector_clock.clone(),
        };

        self.publish_data_with_clock(data_with_clock).await
    }

    async fn publish_data_with_clock(&mut self, data_with_clock: DataWithClock) -> Result<(), Box<dyn Error>> {
        if let Ok(data) = serde_json::to_string(&data_with_clock) {
            self.swarm.behaviour_mut().gossipsub.publish(self.topic.clone(), data.as_bytes())?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    println!("Local peer id: {:?}", peer_id);

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

    let config = Config::with_executor(Box::new(|fut| {
        tokio::spawn(fut);
    }));

    let mut swarm = Swarm::new(transport, behaviour, peer_id, config);

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut node = Node::new(swarm);
    node.start().await?;

    Ok(())
}