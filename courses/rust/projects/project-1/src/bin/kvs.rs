use std::{env::current_dir, process::exit};

use anyhow::Context;
use clap::Parser;
use flume;
use monoio;
use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
};
use serde::{Deserialize, Serialize};
use serde_json;

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

#[derive(Serialize, Deserialize, Debug)]
enum StoreReq {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    Rm { key: Vec<u8> },
}

#[derive(Serialize, Deserialize, Debug)]
enum StoreRep {
    GetSuccess(Vec<u8>),
    SetSuccess,
    RmSuccess,
    Error(String),
}

async fn loop_store(
    mut store: KvStore,
    tx: flume::Sender<StoreRep>,
    rx: flume::Receiver<StoreReq>,
) -> Result<()> {
    loop {
        let rep = match rx.recv_async().await? {
            StoreReq::Get { key } => match store.get(key.as_slice()).await {
                Ok(Some(value)) => StoreRep::GetSuccess(value),
                Ok(None) => StoreRep::Error("Key not found".to_string()),
                Err(err) => StoreRep::Error(err.to_string()),
            },
            StoreReq::Set { key, value } => match store.set(key, value).await {
                Ok(_) => StoreRep::SetSuccess,
                Err(err) => StoreRep::Error(err.to_string()),
            },
            StoreReq::Rm { key } => match store.remove(key.as_slice()).await {
                Err(KvsError::KeyNotFound) => StoreRep::Error("Key not found".to_string()),
                Err(err) => StoreRep::Error(err.to_string()),
                _ => StoreRep::RmSuccess,
            },
        };

        tx.send_async(rep).await.context("flume::SendError<StoreRep>>")?;
    }
}

async fn handle_stream(
    mut stream: TcpStream,
    tx: flume::Sender<StoreReq>,
    rx: flume::Receiver<StoreRep>,
) -> Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut read;
    let mut written;
    loop {
        (read, buf) = stream.read(buf).await;
        if read? == 0 {
            let msg: StoreReq = serde_json::from_slice(buf.as_slice())?;
            tx.send_async(msg).await.context("chan send error")?;
            let resp = rx.recv_async().await.context("chan recv errro")?;
            buf = serde_json::to_vec(&resp)?;

            (written, buf) = stream.write_all(buf).await;
            written?;
        }
    }
}

async fn loop_tcp(
    addr: std::net::SocketAddr,
    tx: flume::Sender<StoreReq>,
    rx: flume::Receiver<StoreRep>,
) -> Result<()> {
    let listener = TcpListener::bind(addr)?;

    loop {
        // TODO: multiple connections
        // TODO: graceful shutdown
        let stream = listener.accept().await?.0;
        // TODO: error handling
        monoio::spawn(handle_stream(stream, tx.clone(), rx.clone()));
    }
}

#[monoio::main(enable_timer = true)]
async fn main() -> Result<()> {
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
            let task_store = monoio::spawn(loop_store(store, rep_tx, req_rx));
            let task_serve = monoio::spawn(loop_tcp(addr, req_tx, rep_rx));
            task_store.await?;
            task_serve.await?;
        }
        Args::Compact => {
            todo!()
        }
    };

    Ok(())
}
