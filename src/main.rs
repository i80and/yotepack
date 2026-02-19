use std::path::PathBuf;

use camino::Utf8Path;
use clap::{Parser, Subcommand};

pub mod block_capnp {
    include!(concat!(env!("OUT_DIR"), "/block_capnp.rs"));
}

pub mod txnlog_capnp {
    include!(concat!(env!("OUT_DIR"), "/txnlog_capnp.rs"));
}

mod fsutil;
mod storage_engine;
mod transaction_log;

#[derive(Parser)]
#[command(name = "yotepack")]
#[command(about = "A CLI tool for disk management")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create operation on disks
    Create {
        /// List of disk paths
        disks: Vec<PathBuf>,
    },
    /// Serve operation on disks
    Serve {
        /// List of disk paths
        disks: Vec<PathBuf>,
    },
    /// Resize operation on disks
    Resize {
        /// List of disk paths
        disks: Vec<PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Create { disks } => {
            println!("Creating with disks: {:?}", disks);
            storage_engine::StorageEngine::create(disks.as_slice(), (2, 1))?;
        }
        Commands::Serve { disks } => {
            println!("Serving with disks: {:?}", disks);
            let engine = storage_engine::StorageEngine::load(disks.as_slice())?;
            engine.put(
                Utf8Path::new("foobar"),
                &mut std::io::Cursor::new(b"hello world"),
            )?;
            let mut out_buf = vec![];
            engine.get(Utf8Path::new("foobar"), &mut out_buf)?;
            assert_eq!(out_buf, b"hello world");
            assert_eq!(
                engine.list(Utf8Path::new("foobar"))?,
                vec![Utf8Path::new("foobar")]
            );
        }
        Commands::Resize { disks } => {
            println!("Resizing with disks: {:?}", disks);
            unimplemented!("aaaaugh");
        }
    }

    Ok(())
}
