use std::env::current_dir;
use std::process::exit;

use clap;
use clap::Parser;

use kvs;
use kvs::{KvStore, KvsError, Result};

#[derive(clap::Subcommand)]
enum Cmd {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

#[derive(clap::Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

fn main() -> Result<()> {
    let mut store = KvStore::open(current_dir()?)?;
    match Args::parse().cmd {
        Cmd::Get { key } => match store.get(key.to_string())? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        Cmd::Set { key, value } => {
            store.set(key.to_string(), value.to_string())?;
        }
        Cmd::Rm { key } => match store.remove(key) {
            Ok(()) => {}
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1);
            }
            Err(e) => return Err(e),
        },
    };

    Ok(())
}
