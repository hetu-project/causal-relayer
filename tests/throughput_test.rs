use reqwest;
use serde_json::{json, Value};
use rand::Rng;
use std::time::Instant;
use futures::future::join_all;
use tokio::time::{sleep, Duration};

fn generate_random_hex(length: usize) -> String {
    const HEX_CHARS: &[u8] = b"0123456789abcdef";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..HEX_CHARS.len());
            HEX_CHARS[idx] as char
        })
        .collect()
}

async fn send_request(client: &reqwest::Client, url: &str, method: &str, params: Value) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let response = client
        .post(url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": rand::random::<u32>()
        }))
        .send()
        .await?;

    Ok(response.json().await?)
}

#[tokio::test]
async fn test_parallel_rpc_throughput() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let url = "http://127.0.0.1:30000"; // Adjust this to match your RPC server's address
    let drain_url = "http://192.168.0.104:30000"; // Adjust this to match your RPC server's address
    let num_requests = 1000;
    let concurrent_requests = 20; // Number of concurrent requests

    println!("Starting parallel throughput test with {} total requests, {} at a time", num_requests, concurrent_requests);

    let start_time = Instant::now();

    let mut all_results = Vec::new();

    for chunk in (0..num_requests).collect::<Vec<_>>().chunks(concurrent_requests) {
        let futures = chunk.iter().map(|_| {
            let random_hex = generate_random_hex(64); // 32 bytes = 64 hex characters
            let params = json!([format!("0x{}", random_hex)]);
            let client = &client;
            let url = url.to_string();

            async move {
                send_request(client, &url, "eth_sendRawTransaction", params).await
            }
        });

        let results = join_all(futures).await;
        all_results.extend(results);

        // Add a small delay between chunks to avoid overwhelming the server
        // sleep(Duration::from_millis(100)).await;
    }

    let send_elapsed = start_time.elapsed();
    let send_speed = num_requests as f64 / send_elapsed.as_secs_f64();

    println!("Send throughput test completed in {:?}", send_elapsed);
    println!("Send speed: {:.2} requests/second", send_speed);

    // Test drain_data after sending all requests
    sleep(Duration::from_secs(10)).await;
    let drain_start = Instant::now();
    match send_request(&client, drain_url, "drain_data", json!([])).await {
        Ok(drain_result) => {
            let drain_elapsed = drain_start.elapsed();

            if let Some(result) = drain_result.get("result") {
                if let Some(drained_data) = result.as_array() {
                    let drained_count = drained_data.len();
                    let drain_speed = drained_count as f64 / drain_elapsed.as_secs_f64();
                    println!("Drained data count: {}", drained_count);
                    println!("Drain speed: {:.2} items/second", drain_speed);

                    // Verify that all sent data was drained
                    assert_eq!(drained_count, num_requests, "Not all sent data was drained");
                } else {
                    println!("Unexpected drain_data result format. Expected an array.");
                }
            } else {
                println!("Unexpected drain_data response format. 'result' field not found.");
            }
        },
        Err(e) => println!("drain_data request failed: {:?}", e),
    }

    Ok(())
}