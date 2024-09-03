use futures::StreamExt;
use libp2p::{core::upgrade, gossipsub, identity, mdns, noise, swarm::{SwarmEvent, Swarm}, tcp, Multiaddr, PeerId, Transport};
use libp2p_swarm_derive::NetworkBehaviour;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use jsonrpc_core::{IoHandler, Params, Value};
use jsonrpc_http_server::ServerBuilder;
use std::net::SocketAddr;
use std::process;

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "NodeEvent")]
struct NodeBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

enum NodeEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<gossipsub::Event> for NodeEvent {
    fn from(event: gossipsub::Event) -> Self {
        NodeEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for NodeEvent {
    fn from(event: mdns::Event) -> Self {
        NodeEvent::Mdns(event)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DataWithClock {
    data: String,
    vector_clock: HashMap<String, u64>,
}

struct Node {
    swarm: Swarm<NodeBehaviour>,
    vector_clock: HashMap<PeerId, u64>,
    topic: gossipsub::IdentTopic,
}

impl Node {
    fn new(swarm: Swarm<NodeBehaviour>) -> Self {
        Node {
            swarm,
            vector_clock: HashMap::new(),
            topic: gossipsub::IdentTopic::new("relay_data"),
        }
    }

    async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::Behaviour(NodeEvent::Gossipsub(gossipsub::Event::Message {
                                                               propagation_source,
                                                               message_id,
                                                               message
                                                           })) => {
                    if let Ok(data) = serde_json::from_slice::<DataWithClock>(&message.data) {
                        self.process_received_data(data).await?;
                    }
                }
                _ => {}
            }
        }
    }

    async fn process_received_data(&mut self, mut data: DataWithClock) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (peer, &clock) in &data.vector_clock {
            let peer_id = PeerId::from_bytes(&hex::decode(peer)?)?;
            self.vector_clock.entry(peer_id).and_modify(|e| *e = std::cmp::max(*e, clock)).or_insert(clock);
        }

        let local_peer_id = *self.swarm.local_peer_id();
        *self.vector_clock.entry(local_peer_id).or_insert(0) += 1;

        data.vector_clock = self.vector_clock.iter().map(|(k, v)| (hex::encode(k.to_bytes()), *v)).collect();

        self.publish_data(data).await
    }

    async fn publish_data(&mut self, data: DataWithClock) -> Result<(), Box<dyn Error + Send + Sync>> {
        let message = serde_json::to_string(&data)?;
        self.swarm.behaviour_mut().gossipsub.publish(self.topic.clone(), message.as_bytes())?;
        Ok(())
    }

    async fn connect_to_peer(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.swarm.dial(addr.clone())?;
        Ok(())
    }
}

async fn run_json_rpc_server(address: SocketAddr, tx: mpsc::Sender<String>) {
    let mut io = IoHandler::new();

    io.add_sync_method("submit_data", move |params: Params| {
        let tx = tx.clone();
        let data = params.parse::<String>().unwrap_or_default();
        tokio::spawn(async move {
            if let Err(e) = tx.send(data).await {
                error!("Failed to send data: {:?}", e);
            }
        });
        Ok(Value::String("Data submitted successfully".to_string()))
    });

    let server = ServerBuilder::new(io)
        .start_http(&address)
        .expect("Unable to start RPC server");

    info!("JSON-RPC server listening on http://{}", address);
    server.wait();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let (p2p_port, rpc_port) = parse_args()?;
    let (mut node, peer_id) = create_node(p2p_port).await?;

    let (tx, mut rx) = mpsc::channel::<String>(100);

    let rpc_address = format!("127.0.0.1:{}", rpc_port).parse()?;
    tokio::spawn(run_json_rpc_server(rpc_address, tx.clone()));

    info!("P2P node {} listening on /ip4/0.0.0.0/tcp/{}", peer_id, p2p_port);

    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
    let shutdown_signal = async move {
        tokio::signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
        shutdown_sender.send(()).expect("Failed to send shutdown signal");
    };

    tokio::select! {
        _ = shutdown_signal => {
            info!("Received Ctrl+C, shutting down");
        }
        _ = shutdown_receiver => {
            info!("Shutdown signal received");
        }
        result = run_node(&mut node, &mut rx) => {
            if let Err(e) = result {
                error!("Node error: {:?}", e);
            }
        }
    }

    // Perform any necessary cleanup here
    info!("Cleanup complete, exiting");
    process::exit(0);
}

async fn run_node(node: &mut Node, rx: &mut mpsc::Receiver<String>) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        tokio::select! {
            result = node.start() => {
                if let Err(e) = result {
                    error!("Node error: {:?}", e);
                    return Err(e.into());
                }
            }
            Some(message) = rx.recv() => {
                if let Err(e) = node.publish_data(DataWithClock {
                    data: message,
                    vector_clock: node.vector_clock.iter().map(|(k, v)| (hex::encode(k.to_bytes()), *v)).collect(),
                }).await {
                    error!("Failed to publish data: {:?}", e);
                }
            }
        }
    }
}

fn parse_args() -> Result<(u16, u16), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err("Usage: <program> <p2p_port> <rpc_port>".into());
    }
    Ok((args[1].parse()?, args[2].parse()?))
}

async fn create_node(port: u16) -> Result<(Node, PeerId), Box<dyn Error + Send + Sync>> {
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
            gossipsub_config,
        ).expect("Valid configuration");

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
            .expect("Valid config");

        NodeBehaviour { gossipsub, mdns }
    };

    let config = libp2p::swarm::Config::with_tokio_executor();
    let mut swarm = Swarm::new(transport, behaviour, peer_id, config);

    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", port).parse()?)?;

    let node = Node::new(swarm);

    Ok((node, peer_id))
}