#![allow(missing_docs)]

use std::os::unix::prelude::OpenOptionsExt;
use std::{collections::HashMap, path::PathBuf};

use crate::util::thread_of;
use crate::util::ParseId;
use crate::xchg::{Command, Index};
use crate::{
    fs::{Reader, Writer},
    xchg::StreamReader,
    Result,
};

use anyhow::Context;
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use libc;
use monoio::fs::OpenOptions;

#[cfg(target_os = "linux")]
const O_DIRECT: libc::c_int = libc::O_DIRECT;
#[cfg(target_os = "macos")]
const O_DIRECT: libc::c_int = 0;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    #[allow(dead_code)]
    path: std::path::PathBuf,
    writers: HashMap<u64, (Writer, u64)>,
    readers: HashMap<u64, (Reader, u64)>,
    map: SkipMap<Vec<u8>, Index>,
}

// TODO: bloom filter -> cache -> ptr map -> disk (O_DIRECT)
// https://www.usenix.org/sites/default/files/conference/protected-files/fast21_slides_zhong.pdf
impl KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let thread_id = thread_of(key.as_slice());
        let (writer, file_id) = self.writers.get_mut(&thread_id).unwrap();
        let timestamp = Utc::now();
        let pos = writer.pos as u64;

        let cmd = Command::Write {
            key: key.to_vec(),
            value,
            timestamp,
        };

        let written = cmd.to_writer(writer).await?;

        let index = Index {
            file_id: *file_id,
            pos,
            len: written as _,
            timestamp,
        };
        self.map.insert(key, index);

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let option = self
            .map
            .get(key)
            .map(|entry| {
                let reader = self
                    .readers
                    .get(&entry.value().file_id)
                    .expect(&format!("file_id={} not found", entry.value().file_id)).0;
                (reader, entry.value().len, entry.value().pos)
            })
            .map(|(mut reader, len, pos)| async move {
                let buf = vec![0u8; len as usize];
                let (res, buf) = reader.pread_exact(buf, pos).await;
                res.map(|_| buf)
            });

        match option {
            Some(fut) => Ok(Some(fut.await?)),
            None => Ok(None),
        }
    }

    /// Remove a given key.
    pub async fn remove(&mut self, key: &[u8]) -> Result<()> {
        // self.map
        //     .get(key)
        //     .context("Key not found")
        //     .map(|_| async {
        //         let cmd = Command::Delete {
        //             key: key.to_vec(),
        //             timestamp: Utc::now(),
        //         };
        //         cmd.to_writer(&mut self.writer).await
        //     })?
        //     .await?;
        //
        // self.map.remove(key);

        Ok(())
    }

    /// persist the index to disk
    // async fn save(&self) -> Result<()> {
    //     todo!()
    // }

    /// merge immutable log files
    pub async fn merge(&self) -> Result<()> {
        unimplemented!()
    }

    pub async fn load<P: AsRef<std::path::Path>>(
        paths: impl Iterator<Item = P>,
    ) -> std::io::Result<SkipMap<Vec<u8>, Index>> {
        let map = SkipMap::<Vec<u8>, Index>::new();

        for path in paths {
            let file_id = path.as_ref().parse_id();
            let file = OpenOptions::new().read(true).open(path).await?;
            let mut reader = StreamReader::new(file);

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
                            Index {
                                file_id,
                                pos: prev_pos,
                                len: reader.cursor() - prev_pos,
                                timestamp,
                            },
                        );
                    }
                    Command::Delete { key, .. } => {
                        map.remove(&key);
                    }
                }

                prev_pos = reader.cursor();
            }
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

        let map = Self::load(paths.iter()).await?;

        paths.sort_unstable_by_key(|buf| buf.as_path().parse_id());

        let file_id_to_write = paths
            .iter()
            .last()
            .map(|s| s.parse_id())
            .map_or(0, |x| x + 1);

        let writer = OpenOptions::new()
            .create_new(true)
            .append(true)
            .custom_flags(O_DIRECT)
            .open(&path.join(format!("{file_id_to_write}.log")))
            .await
            .map(Writer::new)?;

        let mut readers = HashMap::<u64, Reader>::new();
        for buf in paths.iter() {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(O_DIRECT)
                .open(buf.clone())
                .await?;
            readers.insert(buf.as_path().parse_id(), Reader::new(file));
        }

        Ok(Self {
            path: path.into(),
            writer,
            active_file_id: file_id_to_write,
            readers,
            map,
        })
    }
}
