use clap::Parser;
use shoebox::config::{resolve_all_buckets, ServerConfig};

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

    // Print startup info
    println!(
        "Serving {} bucket{} on http://{}:{}",
        buckets.len(),
        if buckets.len() == 1 { "" } else { "s" },
        config.host,
        config.port
    );
    println!();

    for bucket in &buckets {
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

    if let Some(ref data_dir) = config.data_dir {
        println!("Credentials saved to {}/*/config.toml", data_dir.display());
    } else {
        println!("Credentials saved to .shoebox/config.toml");
    }
    if !config.show_secrets {
        println!("Use --show-secrets to display secret access keys");
    }

    // TODO: Add the Axum router and server here
    tracing::info!("Server startup complete (no HTTP listener yet — Phase 2)");

    Ok(())
}
