use config::{Config, ConfigBuilder, Environment, File, FileFormat};
use std::env;
use std::error::Error;
use std::path::Path;
use tracing::{error, info, warn};

pub fn load_config(config_path: &str) -> Result<Config, Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();

    let mut builder = ConfigBuilder::<config::builder::DefaultState>::default();

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
        builder = builder.add_source(File::new(config_path, FileFormat::Toml).required(true));
        info!("Added config file source");
    } else {
        warn!(
            "Config file not found at: {}. Using default values.",
            config_path
        );
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