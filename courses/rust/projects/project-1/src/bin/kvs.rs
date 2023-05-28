use std::{env::current_dir, process::exit};

use clap::Parser;
use flume;
use monoio;
use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
};

use kvs::{KvStore, KvsError, Result};

#[derive(clap::Subcommand)]
enum Cmd {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
    Serve,
    Compact,
}

#[derive(clap::Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

async fn process_cli() -> Result<()> {
    let mut store = KvStore::open(current_dir()?).await?;

    Ok(())
}

enum Message {
    Get { key: Vec<u8> },
    Set,
    Rm,
}

async fn loop_store(tx: flume::Sender<Message>, rx: flume::Receiver<Message>) -> Result<()> {
    loop {
        rx.recv_async().await
    }
}

async fn serve() {
    let (client_tx, server_rx) = flume::unbounded::<Message>();
    let (server_tx, client_tx) = flume::unbounded::<Message>();
    monoio::spawn(async {
        loop_store(server_tx, server_tx);
    })
}

async fn handle_stream(
    stream: TcpStream,
    tx: flume::Sender<Message>,
    rx: flume::Receiver<Message>,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    loop {
        let (read, buf) = stream.read(buf).await;
        if read? == 0 {
            Message::from(buf);
            tx.send_async(msg).await?;
            let resp = rx.recv_async().await?;
            let new_buf = magic(resp);

            let (written, buf) = stream.write_all(new_buf).await;
            written?;
        }
    }
}

async fn loop_tcp(tx: flume::Sender<Message>, rx: flume::Receiver<Message>) {
    let listener = TcpListener::bind("localhost:5000")?;

    loop {
        // TODO: multiple connections
        let stream = listener.accept().await?.0;
        handle_stream(stream, tx, rx).await
    }
}

#[monoio::main(enable_timer = true)]
async fn main() -> Result<()> {
    let cmd = Args::parse().cmd;
    match cmd {
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
        _ => {
            unimplemented!("");
        }
    };

    process_cli().await?;
    Ok(())
}
