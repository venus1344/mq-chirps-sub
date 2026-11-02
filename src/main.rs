mod client;
mod config;
mod db;
mod downlink;
mod events;
mod handler;
mod handlers;
mod topic;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::Config;
use handler::Registry;
use handlers::sqlite::SqliteHandler;
use handlers::stdout::StdoutHandler;

#[derive(Parser)]
#[command(
    name = "mq-subscriber",
    version,
    about = "ChirpStack MQTT subscriber and downlink sender"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the subscriber and receive device events (default).
    Subscribe,
    /// Publish a downlink command to a device and exit.
    Downlink {
        /// Application ID. Defaults to MQ_APPLICATION_ID.
        #[arg(long)]
        application_id: Option<String>,
        /// Device EUI (16 hex characters).
        #[arg(long)]
        dev_eui: String,
        /// FPort (must be > 0).
        #[arg(long)]
        fport: u8,
        /// Payload as base64. Mutually exclusive with --data-hex.
        #[arg(long, conflicts_with = "data_hex", required_unless_present = "data_hex")]
        data: Option<String>,
        /// Payload as hex. Mutually exclusive with --data.
        #[arg(long, conflicts_with = "data", required_unless_present = "data")]
        data_hex: Option<String>,
        /// Send as a confirmed downlink.
        #[arg(long)]
        confirmed: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("mq_subscriber=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let config = Config::from_env()?;

    match cli.command.unwrap_or(Command::Subscribe) {
        Command::Subscribe => run_subscriber(config).await,
        Command::Downlink {
            application_id,
            dev_eui,
            fport,
            data,
            data_hex,
            confirmed,
        } => run_downlink(config, application_id, dev_eui, fport, data, data_hex, confirmed).await,
    }
}

async fn run_subscriber(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = Registry::new();
    if config.enable_stdout {
        registry.register(Box::new(StdoutHandler));
    }
    if config.enable_sqlite {
        let pool = db::init_pool(&config.sqlite_path).await?;
        registry.register(Box::new(SqliteHandler::new(pool)));
    }
    if registry.is_empty() {
        info!("no handlers enabled; events will be decoded and dropped");
    }

    let registry = Arc::new(registry);
    client::run(config, registry).await?;
    Ok(())
}

async fn run_downlink(
    config: Config,
    application_id: Option<String>,
    dev_eui: String,
    fport: u8,
    data: Option<String>,
    data_hex: Option<String>,
    confirmed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let application_id = application_id
        .or(config.application_id.clone())
        .ok_or(downlink::DownlinkError::MissingApplicationId)?;

    // Exactly one of --data / --data-hex is guaranteed present by clap.
    let data_b64 = match (data, data_hex) {
        (Some(b64), None) => b64, // base64 validated inside publish()
        (None, Some(hex)) => {
            let bytes = downlink::decode_hex(&hex)?;
            downlink::encode_base64(&bytes)
        }
        _ => unreachable!("clap requires exactly one of --data / --data-hex"),
    };

    downlink::publish(&config, &application_id, &dev_eui, fport, data_b64, confirmed).await?;
    Ok(())
}
