use std::cell::OnceCell;
use std::{env::current_dir, process::exit};

use clap::Parser;
use monoio;

use kvs::{KvStore, KvsError, Result};

pub static NUM_THREADS: OnceCell<usize> = OnceCell::new();

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

#[monoio::main]
async fn main() -> Result<()> {
    NUM_THREADS.set(std::cmp::max(1, num_cpus::get_physical() - 1));

    let mut store = KvStore::open(current_dir()?).await?;
    match Args::parse().cmd {
        Cmd::Get { key } => match store.get(key.as_bytes()).await? {
            Some(value) => println!("{}", unsafe { String::from_utf8_unchecked(value) }),
            None => println!("Key not found"),
        },
        Cmd::Set { key, value } => {
            store.set(key.into_bytes(), value.into_bytes()).await?;
        }
        Cmd::Rm { key } => match store.remove(key.as_bytes()).await {
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1);
            }
            Err(e) => return Err(e),
            _ => {}
        },
    };

    Ok(())
}
