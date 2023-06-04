#![allow(missing_docs)]

use anyhow::Context;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::RwLock;
use std::{collections::HashMap, path::PathBuf};

use crate::util::ParseId;
use crate::xchg::{Command, Index};
use crate::KvsError;
use crate::{fs::File, xchg::StreamReader, Result};

use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use libc;
use monoio::fs::OpenOptions;
use bytes::{Bytes, BytesMut, Buf};

#[cfg(target_os = "linux")]
const O_DIRECT: libc::c_int = libc::O_DIRECT & 0; // FIXME: ailgnment
#[cfg(target_os = "macos")]
const O_DIRECT: libc::c_int = 0;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
pub(crate) struct Store {
    #[allow(dead_code)]
    path: std::path::PathBuf,
    active_fid: u64,
    files: HashMap<u64, File>,
    map: SkipMap<Bytes, std::sync::RwLock<Index>>,
    // compacting: AtomicBool,
}

// TODO: bloom filter -> cache -> ptr map -> disk (O_DIRECT)
// https://www.usenix.org/sites/default/files/conference/protected-files/fast21_slides_zhong.pdf
impl Store {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub async fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        let timestamp = Utc::now();
        let pos = self.files[&self.active_fid].pos as u64;
        let cmd = Command::Write {
            key: key.clone(),
            value,
            timestamp,
        };

        let written = cmd
            .to_file(self.files.get_mut(&self.active_fid).unwrap())
            .await?;

        let index = Index {
            file_id: self.active_fid,
            pos,
            len: written as _,
            timestamp,
        };
        self.map.insert(key, RwLock::new(index));
        self.files.get_mut(&self.active_fid).unwrap().pos = pos as usize + written;

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub async fn get(&mut self, key: Bytes) -> Result<Option<Bytes>> {
        let option = self
            .map
            .get(&key)
            .map(|entry| {
                let file = self
                    .files
                    .get(&entry.value().read().unwrap().file_id)
                    .expect(&format!(
                        "file_id={} not found",
                        entry.value().read().unwrap().file_id
                    ));
                (
                    file,
                    entry.value().read().unwrap().len,
                    entry.value().read().unwrap().pos,
                )
            })
            .map(|(file, len, pos)| async move {
                let buf = BytesMut::with_capacity(len as _);
                let (res, buf) = file.pread_exact(buf, pos).await;
                res.map(|_| buf)
            });

        // | crc | write/delete | timestamp | ksz | value_sz | key | value |
        // | u32 | u8           | i64       | usz | usz      | _   | _     |
        match option {
            Some(fut) => {
                let buf = fut.await?.freeze();
                let start = 4 + 1 + 8;
                let ksz = buf.slice(start..start + 8).get_u64_le();
                Ok(Some(buf.slice(start + 8 + 8 + ksz as usize..)))
            }
            None => Ok(None),
        }
    }

    /// Remove a given key.
    pub async fn remove(&mut self, key: Bytes) -> Result<()> {
        self.map
            .get(&key)
            .ok_or(KvsError::KeyNotFound)
            .map(|_| async {
                let cmd = Command::Delete {
                    key: key.clone(),
                    timestamp: Utc::now(),
                };
                cmd.to_file(self.files.get_mut(&self.active_fid).unwrap())
                    .await
            })?
            .await?;

        self.map.remove(&key);

        Ok(())
    }

    // fn compactable(&self) -> bool {
    // garbage size
    // data size
    // in progress?
    // }

    // async fn try_compact(&mut self) {
    //
    //     let fut = monoio::spawn(async {
    //         // if self.compacting.load(Ordering::SeqCst) {
    //         //     return;
    //         // }
    //         // self.compacting.store(true, Ordering::SeqCst);
    //         // self.compacting.store(false, Ordering::SeqCst);
    //
    //         self.compact(0);
    //     });
    // }

    async fn compact(&mut self, file_id: u64) -> std::io::Result<()> {
        // load everything in memory.
        // this is disgusting. if only we had SSTabls, we could do partial compaction recursively
        // on disk
        //
        // what about the file being appended?

        let mut map = SkipMap::<Bytes, RwLock<Index>>::new();
        let file = &self.files[&file_id];
        Self::load_file(file, &mut map).await?;
        // map.iter().map(|entry| {});

        Ok(())
    }

    // fn mergeable(&self) -> bool {}

    /// persist the index to disk
    // async fn save(&self) -> Result<()> {
    //     todo!()
    // }

    /// merge immutable log files
    pub async fn merge(&self, lhs: File, rhs: File) -> Result<()> {
        let mut lreader = StreamReader::<Command>::new(&lhs);
        let mut rreader = StreamReader::<Command>::new(&rhs);

        // new file

        // let map = SkipMap::<Bytes, RwLock<Index>>::new();
        let mut lhs = lreader.read_entry().await?;
        let mut rhs = rreader.read_entry().await?;
        while lhs.is_some() && rhs.is_some() {
            if lhs <= rhs {

            } else {

            }
            lhs = lreader.read_entry().await?;
            rhs = rreader.read_entry().await?;
        }

        while lhs.is_some() {
            lhs = lreader.read_entry().await?;
        }

        while rhs.is_some() {
            rhs = lreader.read_entry().await?;
        }

        // rm lfile
        // rm rfile
        Ok(())
    }

    async fn load_file(
        file: &File,
        map: &mut SkipMap<Bytes, RwLock<Index>>,
    ) -> std::io::Result<()> {
        let file_id = file.path.as_path().parse_id();
        let mut reader = StreamReader::new(&file);

        let mut prev_pos = 0u64;
        while let Some(cmd) = reader.read_entry().await? {
            match cmd {
                Command::Write {
                    key,
                    value: _,
                    timestamp,
                } => {
                    map.insert(
                        key,
                        RwLock::new(Index {
                            file_id,
                            pos: prev_pos,
                            len: ({
                                let ref this = reader;
                                this.cursor
                            }) - prev_pos,
                            timestamp,
                        }),
                    );
                }
                Command::Delete { key, .. } => {
                    map.remove(&key);
                }
            }

            prev_pos = {
                let ref this = reader;
                this.cursor
            };
        }

        Ok(())
    }

    pub async fn load<P: AsRef<std::path::Path>>(
        paths: impl Iterator<Item = P>,
    ) -> std::io::Result<SkipMap<Bytes, RwLock<Index>>> {
        let mut map = SkipMap::<Bytes, RwLock<Index>>::new();

        for path in paths {
            let file = File::new(
                path.as_ref().to_path_buf().clone(),
                OpenOptions::new().read(true).open(&path).await?,
            );
            Self::load_file(&file, &mut map).await?;
        }

        Ok(map)
    }

    /// Creates a `KvStore`.
    pub async fn open(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        std::fs::create_dir_all(&path)?; // FIXME: blocking

        let mut paths: Vec<PathBuf> = std::fs::read_dir(&path)? // FIXME: blocking
            .flat_map(|entry| entry.map(|x| x.path()))
            .filter(|path| path.is_file())
            .filter(|path| path.extension() == Some("log".as_ref()))
            .collect();

        paths.sort_unstable_by_key(|buf| buf.as_path().parse_id());
        let map = Self::load(paths.iter()).await?; // TODO: open once only

        // monoio::time::sleep(std::time::Duration::from_secs(10)).await;

        let mut files = HashMap::<u64, File>::new();
        for buf in paths.iter() {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(O_DIRECT)
                .open(buf.clone())
                .await?;
            files.insert(buf.as_path().parse_id(), File::new(buf.clone(), file));
        }

        let file_id_to_write = paths
            .iter()
            .last()
            .map(|s| s.parse_id())
            .map_or(0, |x| x + 1);

        let active_path = path.join(&format!("{file_id_to_write}.log"));
        let active_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .custom_flags(O_DIRECT)
            .open(&active_path)
            .await?;

        files.insert(file_id_to_write, File::new(active_path, active_file));

        Ok(Self {
            path,
            active_fid: file_id_to_write,
            files,
            map,
            // compacting: false,
        })
    }
}

#[derive(Debug)]
pub enum StoreReq {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { key: Bytes },
    // Compact,
}

#[derive(Debug)]
pub enum StoreRep {
    Get(core::result::Result<Option<Bytes>, KvsError>),
    Set(core::result::Result<(), KvsError>),
    Del(core::result::Result<(), KvsError>),
}

pub struct KvStore {
    client_tx: flume::Sender<StoreReq>,
    client_rx: flume::Receiver<StoreRep>,
    kill_tx: flume::Sender<()>,
}

impl KvStore {
    pub async fn open(path: impl Into<std::path::PathBuf>) -> Result<KvStore> {
        let (client_tx, server_rx) = flume::unbounded::<StoreReq>();
        let (server_tx, client_rx) = flume::unbounded::<StoreRep>();
        let (kill_tx, kill_rx) = flume::unbounded::<()>();

        let store = Store::open(path).await?;
        let fut = loop_store(store, server_tx, server_rx, kill_rx);
        monoio::spawn(Box::pin(async move { fut.await }));

        Ok(Self {
            client_tx,
            client_rx,
            kill_tx,
        })
    }

    pub fn cancel(&self) -> Result<()> {
        self.kill_tx
            .send(())
            .context("failed to send kill signal to KvStore")?;
        Ok(())
    }

    pub async fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.client_tx.send_async(StoreReq::Get { key }).await?;

        match self.client_rx.recv_async().await? {
            StoreRep::Get(result) => result,
            _ => unreachable!(),
        }
    }

    pub async fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.client_tx
            .send_async(StoreReq::Set { key, value })
            .await?;

        match self.client_rx.recv_async().await? {
            StoreRep::Set(result) => result,
            _ => unreachable!(),
        }
    }

    pub async fn del(&self, key: Bytes) -> Result<()> {
        self.client_tx.send_async(StoreReq::Del { key }).await?;

        match self.client_rx.recv_async().await? {
            StoreRep::Del(result) => result,
            _ => unreachable!(),
        }
    }
}

impl std::ops::Drop for KvStore {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

async fn loop_store(
    mut store: Store,
    server_tx: flume::Sender<StoreRep>,
    server_rx: flume::Receiver<StoreReq>,
    kill_rx: flume::Receiver<()>,
) -> crate::Result<()> {
    loop {
        monoio::select! {
            _ = kill_rx.recv_async() => return Ok(()),
            rcvd = server_rx.recv_async() => {
                let rep = match rcvd? {
                    StoreReq::Get { key } => StoreRep::Get(store.get(key).await),
                    StoreReq::Set { key, value } => {
                        StoreRep::Set(store.set(key, value).await)
                    }
                    StoreReq::Del { key } => {
                        StoreRep::Del(store.remove(key).await)
                    }
                    // StoreReq::Compact => store.compact(),
                };

                server_tx
                    .send_async(rep)
                    .await
                    .expect("message channel unexpectedly dropped")
            }
        }
    }
}
