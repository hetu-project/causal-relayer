# P2P Data Relay Node

A peer-to-peer (P2P) data relay node built with Rust and libp2p, focusing on maintaining the correct sequence of events across a distributed network.

## Key Features

- P2P communication using libp2p
- Sequence preservation using VLC
- Data synchronization and draining with tombstone mechanism
- JSON-RPC server for easy interaction

## Quick Start

1. Install Rust and Cargo
2. Clone the repository
3. Create a `config.toml` file (see Configuration section)
4. Build and run:

```
cargo build --release
cargo run --release
```

## Configuration

Create a `config.toml` file:

```toml
[network]
p2p_port = 8000
rpc_port = 8001
external_ip = "127.0.0.1"

[node]
private_key = ""  # Leave empty to generate a new key
bootstrap_peers = ["/ip4/127.0.0.1/tcp/8000/p2p/PEER_ID"]
```

## Usage

### Submit Data

```
curl -X POST -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["Your data here"],"id":1}' \
     http://localhost:8001
```

### Drain Data

```
curl -X POST -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","method":"drain_data","params":[],"id":1}' \
     http://localhost:8001
```

## Sequence Functionality

- Uses VLC to timestamp events
- Stores data in ordered structure (BTreeMap)
- Resolves conflicts based on VLC comparisons
- Propagates tombstones to maintain sequence integrity after draining

## Logging

Set log level with the `RUST_LOG` environment variable:

```
RUST_LOG=info cargo run --release
```

## Contributing

Contributions are welcome! Please submit a Pull Request.

## License

This project is open source under the [MIT License](LICENSE).