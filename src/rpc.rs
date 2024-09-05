use std::collections::HashSet;
use jsonrpc_core::{BoxFuture, Error as RpcError, IoHandler, Params, Value};
use jsonrpc_http_server::ServerBuilder;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::runtime::Runtime;
use tracing::{error, info, warn};
use crate::node::Node;

pub fn run_json_rpc_server(address: SocketAddr, tx: mpsc::Sender<String>, node: Arc<Node>) {
    let runtime = Arc::new(Runtime::new().expect("Failed to create Tokio runtime"));
    let mut io = IoHandler::new();

    // Use a HashSet to keep track of submitted data
    let submitted_data = Arc::new(Mutex::new(HashSet::new()));

    io.add_method("submit_data", move |params: Params| {
        let tx = tx.clone();
        let submitted_data = submitted_data.clone();

        Box::pin(async move {
            match params.parse::<Vec<String>>() {
                Ok(data) => {
                    if data.is_empty() {
                        warn!("Received empty data via RPC");
                        return Ok(Value::String("Received empty data".to_string()));
                    }
                    let message = data[0].clone(); // Get the first string from the array

                    // Check if the data has already been submitted
                    let is_new_data = {
                        let mut submitted_set = submitted_data.lock().unwrap();
                        submitted_set.insert(message.clone())
                    };

                    if !is_new_data {
                        warn!("Duplicate data received via RPC: {}", message);
                        return Ok(Value::String("Duplicate data rejected".to_string()));
                    }

                    info!("Received new data via RPC: {}", message);
                    match tx.send(message.clone()).await {
                        Ok(_) => {
                            info!("Successfully sent data to channel");
                            Ok(Value::String("Data submitted successfully".to_string()))
                        }
                        Err(e) => {
                            error!("Failed to send data to channel: {:?}", e);
                            // Remove the data from the set if it couldn't be sent
                            let mut submitted_set = submitted_data.lock().unwrap();
                            submitted_set.remove(&message);
                            Err(RpcError::internal_error())
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to parse RPC params: {:?}", e);
                    Err(RpcError::invalid_params("Invalid parameters"))
                }
            }
        }) as BoxFuture<Result<Value, RpcError>>
    });

    io.add_method("drain_data", move |_params: Params| {
        let node = node.clone();

        Box::pin(async move {
            match node.drain_data().await {
                Ok(data) => {
                    let json_data = json!(data);
                    Ok(Value::Array(json_data.as_array().unwrap().clone()))
                }
                Err(e) => {
                    error!("Failed to drain data: {:?}", e);
                    Err(RpcError::internal_error())
                }
            }
        }) as BoxFuture<Result<Value, RpcError>>
    });

    let server = ServerBuilder::new(io)
        .start_http(&address)
        .expect("Unable to start RPC server");

    info!("JSON-RPC server listening on http://{}", address);
    server.wait();
}