use jsonrpc_core::{IoHandler, Params, Value};
use jsonrpc_http_server::ServerBuilder;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::{info, warn, error};

pub async fn run_json_rpc_server(address: SocketAddr, tx: mpsc::Sender<String>) {
    let mut io = IoHandler::new();

    io.add_sync_method("submit_data", move |params: Params| {
        let tx = tx.clone();
        match params.parse::<Vec<String>>() {
            Ok(data) => {
                if data.is_empty() {
                    warn!("Received empty data via RPC");
                    return Ok(Value::String("Received empty data".to_string()));
                }
                let message = data[0].clone(); // Get the first string from the array
                info!("Received data via RPC: {}", message);
                tokio::spawn(async move {
                    if let Err(e) = tx.send(message).await {
                        error!("Failed to send data to channel: {:?}", e);
                    } else {
                        info!("Successfully sent data to channel");
                    }
                });
                Ok(Value::String("Data submitted successfully".to_string()))
            }
            Err(e) => {
                error!("Failed to parse RPC params: {:?}", e);
                Err(jsonrpc_core::Error::invalid_params("Invalid parameters"))
            }
        }
    });

    let server = ServerBuilder::new(io)
        .start_http(&address)
        .expect("Unable to start RPC server");

    info!("JSON-RPC server listening on http://{}", address);
    server.wait();
}