use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use shoebox::api::routes::create_router;
use shoebox::config::{resolve_all_buckets, ServerConfig, METADATA_DB};
use shoebox::metadata::MetadataStore;
use shoebox::services::{AppState, LoadedBucket};
use shoebox::storage::FilesystemStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SHOEBOX_LOG")
                .or_else(|_| tracing_subscriber::EnvFilter::try_from_env("RUST_LOG"))
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = ServerConfig::parse();
    let buckets = resolve_all_buckets(&config).await?;

    let mut loaded_buckets = HashMap::new();
    for bucket in &buckets {
        // Open metadata database
        let db_path = bucket.shoebox_dir.join(METADATA_DB);
        let metadata = MetadataStore::new(&db_path).await?;

        // Create storage layer
        let storage = FilesystemStorage::new(bucket.root.clone());

        loaded_buckets.insert(
            bucket.name.clone(),
            LoadedBucket {
                name: bucket.name.clone(),
                config: bucket.config.clone(),
                storage,
                metadata,
            },
        );

        // Print bucket info
        let show = config.show_secrets || bucket.freshly_created;
        println!("  {} → {}", bucket.name, bucket.root.display());
        if bucket.freshly_created {
            println!("    (new) Credentials generated:");
        } else {
            println!("    Credentials:");
        }
        for (i, cred) in bucket.config.credentials.iter().enumerate() {
            let desc = cred.description.as_deref().unwrap_or("no description");
            println!("      [{}] {} ({})", i + 1, cred.access_key_id, desc);
            if show {
                println!("          Secret: {}", cred.secret_access_key);
            }
        }
        println!();
    }

    let listen_addr = format!("{}:{}", config.host, config.port);
    println!(
        "Serving {} bucket{} on http://{}",
        loaded_buckets.len(),
        if loaded_buckets.len() == 1 { "" } else { "s" },
        listen_addr,
    );
    if let Some(ref data_dir) = config.data_dir {
        println!("Credentials saved to {}/*/config.toml", data_dir.display());
    } else {
        println!("Credentials saved to .shoebox/config.toml");
    }
    if !config.show_secrets {
        println!("Use --show-secrets to display secret access keys");
    }
    println!();

    let state = AppState {
        buckets: Arc::new(loaded_buckets),
    };

    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    tracing::info!("Listening on {}", listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
