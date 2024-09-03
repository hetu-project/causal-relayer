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
use std::{env, process};
use std::path::Path;
use std::time::Duration;
use config::{Config, ConfigBuilder, Environment, File, FileFormat};
use base64::{Engine as _, engine::general_purpose};
use tokio::time::sleep;

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

    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).cloned().unwrap_or_else(|| "config.toml".to_string());

    let config = load_config(&config_path)?;
    let (mut node, peer_id) = create_node(&config).await?;

    let (tx, mut rx) = mpsc::channel::<String>(100);

    let rpc_port = config.get_int("network.rpc_port")? as u16;
    let rpc_address: SocketAddr = format!("127.0.0.1:{}", rpc_port).parse()?;
    tokio::spawn(run_json_rpc_server(rpc_address, tx.clone()));

    let p2p_port = config.get_int("network.p2p_port")? as u16;
    info!("P2P node {} listening on /ip4/0.0.0.0/tcp/{}", peer_id, p2p_port);

    // Connect to bootstrap peers
    connect_to_bootstrap_peers(&mut node, &config).await?;

    // Wait a bit for the node to establish connections
    sleep(Duration::from_secs(1)).await;

    // Print node addresses
    print_node_addresses(&node, &config).await?;

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
async fn print_node_addresses(node: &Node, config: &Config) -> Result<(), Box<dyn Error + Send + Sync>> {
    let local_peer_id = *node.swarm.local_peer_id();
    let listened_addrs = node.swarm.listeners().cloned().collect::<Vec<_>>();
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
    info!("Full address with external IP: /ip4/{}/tcp/{}/p2p/{}", external_ip, p2p_port, local_peer_id);

    info!("You can share these addresses with others to allow them to connect to your node.");
    Ok(())
}


async fn connect_to_bootstrap_peers(node: &mut Node, config: &Config) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok(bootstrap_peers) = config.get_array("node.bootstrap_peers") {
        for peer in bootstrap_peers {
            if let Ok(addr) = peer.into_string() {
                match addr.parse() {
                    Ok(multiaddr) => {
                        info!("Connecting to bootstrap peer: {}", addr);
                        if let Err(e) = node.connect_to_peer(multiaddr).await {
                            warn!("Failed to connect to bootstrap peer {}: {:?}", addr, e);
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

fn load_config(config_path: &str) -> Result<Config, Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();

    let mut builder = Config::builder();

    // Add default values
    builder = builder
        .set_default("network.p2p_port", 8000)?
        .set_default("network.rpc_port", 8001)?
        .set_default("network.external_ip", "127.0.0.1")?
        .set_default("node.private_key", "")?
        .set_default("storage.path", "./data")?;

    // Check if config file exists and is readable
    let config_file = Path::new(config_path);
    if config_file.exists() {
        info!("Config file found at: {}", config_path);
        let file_source = File::new(config_path, FileFormat::Toml).required(true);
        builder = builder.add_source(file_source);
        info!("Added config file source");
    } else {
        warn!("Config file not found at: {}. Using default values.", config_path);
    }

    // Add environment variables
    builder = builder.add_source(Environment::with_prefix("NODE"));

    // Add command line arguments
    if args.len() > 1 {
        let mut cmd_config = Config::default();
        for arg in args.iter().skip(1) {
            let parts: Vec<&str> = arg.splitn(2, '=').collect();
            if parts.len() == 2 {
                cmd_config.set(parts[0], parts[1])?;
            }
        }
        builder = builder.add_source(cmd_config);
    }

    // Build the config
    let config = match builder.build() {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to build config: {:?}", e);
            return Err(Box::new(e));
        }
    };

    // Log the final configuration
    info!("Final configuration:");
    info!("P2P port: {}", config.get_int("network.p2p_port")?);
    info!("RPC port: {}", config.get_int("network.rpc_port")?);
    info!("External IP: {}", config.get_string("network.external_ip")?);

    Ok(config)
}

async fn run_node(node: &mut Node, rx: &mut mpsc::Receiver<String>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        tokio::select! {
             _ = interval.tick() => {
                let num_peers = node.swarm.connected_peers().count();
                info!("Connected peers: {}", num_peers);
            }
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

async fn create_node(config: &Config) -> Result<(Node, PeerId), Box<dyn Error + Send + Sync>> {
    let private_key_str = config.get_string("node.private_key")?;
    let id_keys = if private_key_str.is_empty() {
        let keys = identity::Keypair::generate_ed25519();
        let encoded_private_key = general_purpose::STANDARD.encode(keys.to_protobuf_encoding()?);
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
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(libp2p::yamux::Config::default())
        .boxed();

    let behaviour = {
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .expect("Valid config");
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(id_keys.clone()),
            gossipsub_config,
        ).expect("Valid configuration");

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
            .expect("Valid config");

        NodeBehaviour { gossipsub, mdns }
    };

    let swarm_config = libp2p::swarm::Config::with_tokio_executor();
    let mut swarm = Swarm::new(transport, behaviour, peer_id, swarm_config);

    let p2p_port = config.get_int("network.p2p_port")? as u16;
    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", p2p_port).parse()?)?;

    let node = Node::new(swarm);

    Ok((node, peer_id))
}