use crate::config::load_config;
use crate::node::Node;
use crate::rpc::run_json_rpc_server;
use std::error::Error;
use std::sync::Arc;
use std::{env, process};
use tokio::sync::mpsc;
use tracing::{error, info};

mod behaviour;
mod config;
mod node;
mod rpc;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let config_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "config.toml".to_string());

    let config = load_config(&config_path)?;
    let (node, peer_id) = Node::create(&config).await?;
    let node = Arc::new(node);

    let (tx, rx) = mpsc::channel::<String>(100);

    let rpc_port = config.get_int("network.rpc_port")? as u16;
    let rpc_address = format!("0.0.0.0:{}", rpc_port).parse()?;

    let rpc_node = node.clone();
    tokio::spawn(async move {
        run_json_rpc_server(rpc_address, tx.clone(), rpc_node);
    });

    let p2p_port = config.get_int("network.p2p_port")? as u16;
    info!(
        "P2P node {} listening on /ip4/0.0.0.0/tcp/{}",
        peer_id, p2p_port
    );

    node.connect_to_bootstrap_peers(&config).await?;
    // Wait a bit for the node to establish connections
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    node.print_node_addresses(&config).await?;

    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
    let shutdown_signal = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        shutdown_sender
            .send(())
            .expect("Failed to send shutdown signal");
    };

    tokio::select! {
        _ = shutdown_signal => {
            info!("Received Ctrl+C, shutting down");
        }
        _ = shutdown_receiver => {
            info!("Shutdown signal received");
        }
        result = node.run(rx) => {
            if let Err(e) = result {
                error!("Node error: {:?}", e);
            }
        }
    }

    process::exit(0);
}
