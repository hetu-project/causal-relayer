use jsonrpc_core::{BoxFuture, Error as RpcError, IoHandler, Params, Value};
use jsonrpc_http_server::ServerBuilder;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::runtime::Runtime;
use tracing::{error, info, warn};
use crate::node::Node;

pub fn run_json_rpc_server(address: SocketAddr, tx: mpsc::Sender<String>, node: Arc<Node>) {
    let runtime = Arc::new(Runtime::new().expect("Failed to create Tokio runtime"));
    let mut io = IoHandler::new();

    let runtime_submit = runtime.clone();
    io.add_method("submit_data", move |params: Params| {
        let tx = tx.clone();
        let runtime = runtime_submit.clone();

        // Return a BoxFuture
        Box::pin(async move {
            match params.parse::<Vec<String>>() {
                Ok(data) => {
                    if data.is_empty() {
                        warn!("Received empty data via RPC");
                        return Ok(Value::String("Received empty data".to_string()));
                    }
                    let message = data[0].clone(); // Get the first string from the array
                    info!("Received data via RPC: {}", message);
                    match tx.send(message).await {
                        Ok(_) => {
                            info!("Successfully sent data to channel");
                            Ok(Value::String("Data submitted successfully".to_string()))
                        }
                        Err(e) => {
                            error!("Failed to send data to channel: {:?}", e);
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

    let runtime_drain = runtime.clone();
    io.add_method("drain_data", move |_params: Params| {
        let node = node.clone();
        let runtime = runtime_drain.clone();

        // Return a BoxFuture
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