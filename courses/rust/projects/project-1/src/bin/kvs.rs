use std::{env::current_dir, process::exit};

use clap::Parser;
use bytes::Bytes;
use monoio;

use kvs::{KvsError, Result, KvStore};

#[derive(clap::Parser)]
#[command(version)]
enum Args {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Rm {
        key: String,
    },

    Serve {
        #[arg(default_value = "localhost:5000")]
        addr: std::net::SocketAddr,
    },
    Compact,
}

#[monoio::main(enable_timer = true)]
async fn main() -> Result<()> {
    let store = KvStore::open(current_dir()?).await?;
    match Args::parse() {
        Args::Get { key } => match store.get(key.into()).await? {
            Some(value) => println!("{}", unsafe { core::str::from_utf8_unchecked(&value) }),
            None => println!("Key not found"),
        },
        Args::Set { key, value } => {
            store.set(Bytes::from(key), Bytes::from(value)).await?;
        }
        Args::Rm { key } => match store.del(key.into()).await {
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1);
            }
            Err(e) => return Err(e),
            _ => {}
        },
        Args::Serve { addr } => {
            // let (req_tx, req_rx) = flume::unbounded::<StoreReq>();
            // let (rep_tx, rep_rx) = flume::unbounded::<StoreRep>();
            // kvs::run_store(store, rep_tx, req_rx).await;
            // kvs::run_server(addr, req_tx, rep_rx).await?;
        }
        Args::Compact => {
            todo!()
        }
    };

    Ok(())
}
