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
    PeerId,
};
use libp2p_swarm_derive::NetworkBehaviour;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::mpsc;
use tokio::signal;

// Define the network behavior for our node
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

// Define the structure for our data with vector clock
#[derive(Clone, Debug)]
struct DataWithClock {
    data: String,
    vector_clock: HashMap<PeerId, u64>,
}

// Define the command enum for the swarm task
enum SwarmCommand {
    Publish(String),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Generate a random PeerId
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    println!("Local peer id: {:?}", peer_id);

    // Create a tokio-based TCP transport use noise for authenticated
    // encryption and Yamux for multiplexing of substreams on a TCP stream.
    let transport = tcp::tokio::Transport::default()
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p-noise static keypair"))
        .multiplex(libp2p::yamux::Config::default())
        .boxed();

    // Create a Swarm to manage peers and events
    let mut swarm = {
        // Set up an encrypted DNS-enabled TCP Transport
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .expect("Valid config");
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(id_keys),
            gossipsub_config
        ).expect("Valid configuration");

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("relay_data");

        // Create a Swarm to manage peers and events
        let mut behaviour = NodeBehaviour {
            gossipsub,
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
                .expect("Valid config"),
        };

        behaviour.gossipsub.subscribe(&topic).expect("Valid topic");

        let config = Config::with_executor(Box::new(|fut| {
            tokio::spawn(fut);
        }));

        Swarm::new(transport, behaviour, peer_id, config)
    };

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Create a channel for RPC service to send data
    let (tx, mut rx) = mpsc::channel::<String>(32);

    // Clone tx for use in RPC method
    let tx_clone = tx.clone();

    // Create an IoHandler and add the RPC method
    let mut io = IoHandler::default();
    io.add_method("submit_data", move |params: Params| {
        let tx = tx_clone.clone();
        async move {
            let data: String = params.parse().unwrap();
            tx.send(data).await.unwrap();
            Ok(Value::String("Data submitted successfully".into()))
        }
    });

    // Start the RPC server
    let server = ServerBuilder::new(io)
        .threads(3)
        .start_http(&"127.0.0.1:30001".parse().unwrap())
        .expect("Unable to start RPC server");

    println!("RPC server listening on port 30001");

    // Create a new channel for sending commands to the swarm task
    let (swarm_tx, mut swarm_rx) = mpsc::channel(32);

    // Spawn a task to handle the swarm events
    let swarm_handle = tokio::spawn(async move {
        let mut swarm = swarm;  // Move swarm into this task
        let topic = gossipsub::IdentTopic::new("relay_data");

        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
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
                            // Process the received data here
                            // Update vector clock and handle data synchronization
                        }
                        _ => {}
                    }
                }
                Some(command) = swarm_rx.recv() => {
                    match command {
                        SwarmCommand::Publish(data) => {
                            if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), data.as_bytes()) {
                                eprintln!("Failed to publish data: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    });

    // Spawn a task to handle incoming RPC data
    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            println!("Received data from RPC: {}", data);
            // Add vector clock and sync with other nodes
            let data_with_clock = DataWithClock {
                data: data.clone(),
                vector_clock: HashMap::new(), // Initialize with empty vector clock
            };
            // Send a command to publish the data
            swarm_tx.send(SwarmCommand::Publish(data)).await.unwrap();
        }
    });

    // Keep the main thread alive
    signal::ctrl_c().await?;
    println!("Shutting down");
    Ok(())
}