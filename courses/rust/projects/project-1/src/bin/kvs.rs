use std::{env::current_dir, process::exit};

use clap::Parser;
use flume;

use kvs::server::{StoreRep, StoreReq};
use kvs::{KvStore, KvsError, Result};

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

// async fn handle_stream(
//     mut stream: TcpStream,
//     tx: flume::Sender<StoreReq>,
//     rx: flume::Receiver<StoreRep>,
// ) -> Result<()> {
//     let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
//     let mut read;
//     let mut written;
//     loop {
//         (read, buf) = stream.read(buf).await;
//         if read? == 0 {
//             let msg: StoreReq = serde_json::from_slice(buf.as_slice())?;
//             tx.send_async(msg).await.context("chan send error")?;
//             let resp = rx.recv_async().await.context("chan recv errro")?;
//             buf = serde_json::to_vec(&resp)?;
//
//             (written, buf) = stream.write_all(buf).await;
//             written?;
//         }
//     }
// }
//
// async fn loop_tcp(
//     addr: std::net::SocketAddr,
//     tx: flume::Sender<StoreReq>,
//     rx: flume::Receiver<StoreRep>,
// ) -> Result<()> {
//     let listener = TcpListener::bind(addr)?;
//
//     loop {
//         // TODO: multiple connections
//         // TODO: graceful shutdown
//         let stream = listener.accept().await?.0;
//         // TODO: error handling
//         spawn(handle_stream(stream, tx.clone(), rx.clone()));
//     }
// }

fn main() -> Result<()> {
    tokio_uring::start(async {
        let mut store = KvStore::open(current_dir()?).await?;
        match Args::parse() {
            Args::Get { key } => match store.get(key.as_bytes()).await? {
                Some(value) => println!("{}", unsafe { String::from_utf8_unchecked(value) }),
                None => println!("Key not found"),
            },
            Args::Set { key, value } => {
                store.set(key.into_bytes(), value.into_bytes()).await?;
            }
            Args::Rm { key } => match store.remove(key.as_bytes()).await {
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
                _ => {}
            },
            Args::Serve { addr } => {
                let (req_tx, req_rx) = flume::unbounded::<StoreReq>();
                let (rep_tx, rep_rx) = flume::unbounded::<StoreRep>();
                kvs::run_store(store, rep_tx, req_rx).await;
                kvs::run_server(addr, req_tx, rep_rx).await?;
            }
            Args::Compact => {
                todo!()
            }
        };

        Ok(())
    })
}
