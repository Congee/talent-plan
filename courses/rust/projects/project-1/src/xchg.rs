#![allow(missing_docs)]

use std::convert::TryInto;
use std::future::Future;
use std::pin::{pin, Pin};
use std::task::Context;
use std::{async_iter::AsyncIterator, task::Poll};

use crate::fs::File;

use chrono::{DateTime, TimeZone as _, Utc};
use crc32fast;

/// | crc | write/delete | timestamp | ksz | key | val_sz | value |
pub enum Command {
    Write {
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: DateTime<Utc>,
    },
    Delete {
        key: Vec<u8>,
        timestamp: DateTime<Utc>,
    },
}

impl Command {
    /// move key & value because io_uring requires a non-volatile buffer to copy
    pub async fn to_file(self, file: &mut File) -> std::io::Result<usize> {
        // | crc | write/delete | timestamp | ksz | value_sz | key | value |
        // | u32 | u8           | i64       | usz | usz      | _   | _     |
        let bufs = match self {
            Command::Write {
                key,
                value,
                timestamp,
            } => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&(0 as u8).to_le_bytes());
                hasher.update(&timestamp.timestamp_nanos().to_le_bytes());
                hasher.update(&(key.len() as u64).to_le_bytes());
                hasher.update(&(value.len() as u64).to_le_bytes());
                hasher.update(&key);
                hasher.update(&value);
                let crc32 = hasher.finalize();

                vec![
                    Vec::from(crc32.to_le_bytes()),
                    Vec::from((0 as u8).to_le_bytes()),
                    Vec::from(timestamp.timestamp_nanos().to_le_bytes()),
                    Vec::from((key.len() as u64).to_le_bytes()),
                    Vec::from((value.len() as u64).to_le_bytes()),
                    key,
                    value,
                ]
            }
            Command::Delete { key, timestamp } => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&(1 as u8).to_le_bytes());
                hasher.update(&timestamp.timestamp_nanos().to_le_bytes());
                hasher.update(&(key.len() as u64).to_le_bytes());
                hasher.update(&key);
                let crc32 = hasher.finalize();

                vec![
                    Vec::from(crc32.to_le_bytes()),
                    Vec::from((1 as u8).to_le_bytes()),
                    Vec::from(timestamp.timestamp_nanos().to_le_bytes()),
                    Vec::from((key.len() as u64).to_le_bytes()),
                    key,
                ]
            }
        };

        file.append(bufs).await
    }
}

/// key -> | file_id | value_sz | value_pos | timestamp |
pub struct Index {
    pub file_id: u64,
    pub pos: u64,
    pub len: u64,
    pub timestamp: DateTime<Utc>,
}

#[allow(unused)]
impl Index {
    async fn to_writer(&self, writer: &mut File) -> std::io::Result<usize> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.file_id.to_le_bytes());
        hasher.update(&self.pos.to_le_bytes());
        hasher.update(&self.len.to_le_bytes());
        hasher.update(&self.timestamp.timestamp_nanos().to_le_bytes());
        let _crc32 = hasher.finalize();

        let bufs = vec![
            Vec::from(self.file_id.to_le_bytes()),
            Vec::from(self.pos.to_le_bytes()),
            Vec::from(self.len.to_le_bytes()),
            Vec::from(self.timestamp.timestamp_nanos().to_le_bytes()),
        ];

        writer.append(bufs).await
    }
}

pub struct StreamReader<'a, T> {
    cursor: u64,
    file: &'a File, // TODO: StreamReader shall be buffered
    __phony: std::marker::PhantomData<T>,
}

impl<'a, T> StreamReader<'a, T> {
    pub fn new(file: &'a File) -> Self {
        Self {
            cursor: 0,
            file,
            __phony: Default::default(),
        }
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }
}

impl<'a> StreamReader<'a, Command> {
    pub(crate) async fn read_entry(&mut self) -> std::io::Result<Option<Command>> {
        // | crc | write/delete | timestamp | ksz | value_sz | key | value |
        // | u32 | u8           | i64       | usz | usz      | _   | _     |
        let bufs = vec![vec![0u8; 4], vec![0u8; 1], vec![0u8; 8], vec![0u8; 8]];
        let mut pos = self.cursor;
        let (result, buf) = self.file.readv_at_all(bufs, Some(pos)).await;
        if let Err(err) = result {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
        }

        let iovec: Vec<Vec<u8>> = buf.into();
        let _crc32 = u32::from_le_bytes(TryInto::<[u8; 4]>::try_into(iovec[0].clone()).unwrap());
        let is_write = iovec[1][0] == 0;
        let ts = i64::from_le_bytes(TryInto::<[u8; 8]>::try_into(iovec[2].clone()).unwrap());
        let timestamp = Utc.timestamp_nanos(ts);
        let ksz = u64::from_le_bytes(TryInto::<[u8; 8]>::try_into(iovec[3].clone()).unwrap());

        pos += 4 + 1 + 8 + 8;

        if is_write {
            let (result, vszbuf) = self.file.inner().read_exact_at(vec![0; 8], pos).await;
            result?;
            let vsz = u64::from_le_bytes(TryInto::<[u8; 8]>::try_into(vszbuf).unwrap());
            pos += 8;

            let bufs = vec![vec![0u8; ksz as _], vec![0u8; vsz as _]];
            let (result, bufs) = self.file.readv_at_all(bufs, Some(pos)).await;
            result?;

            self.cursor = pos + (ksz + vsz) as u64;
            let mut iovec: Vec<Vec<u8>> = bufs.into();

            Ok(Some(Command::Write {
                key: std::mem::replace(&mut iovec[0], Vec::new()),
                value: std::mem::replace(&mut iovec[1], Vec::new()),
                timestamp,
            }))
        } else {
            let key = vec![0; ksz as _];
            let (result, key) = self.file.inner().read_exact_at(key, pos).await;
            result?;
            self.cursor = pos + ksz as u64;

            Ok(Some(Command::Delete { key, timestamp }))
        }
    }
}

impl<'a> AsyncIterator for StreamReader<'a, Command> {
    type Item = std::io::Result<Command>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        pin!(self.read_entry())
            .poll(cx)
            .map(|x| x.map_or_else(|e| Some(Err(e)), |v| v.map(Ok)))
    }
}

// impl<R, T> StreamReader<R, T>
// where
//     R: AsyncReadRentAt + AsyncReadRentExt,
//     T: Sized,
// {
//     pub fn byte_offset(&self) -> u64 {
//         self.0.pos
//     }
//
//
//     fn read_index(&mut self, pos: u64) -> Result<Index> {
//         // TODO: seek from 0
//
//         let mut buf = [0u8; 4];
//         self.0.read_exact(&mut buf)?;
//         let _crc32 = u32::from_le_bytes(buf);
//
//         let mut buf = [0u8; 8];
//         self.0.read_exact(&mut buf)?;
//         let file_id = u64::from_le_bytes(buf);
//
//         let mut buf = [0u8; 8];
//         self.0.read_exact(&mut buf)?;
//         let pos = u64::from_le_bytes(buf);
//
//         let mut buf = [0u8; 8];
//         self.0.read_exact(&mut buf)?;
//         let len = u64::from_le_bytes(buf);
//
//         let mut buf = [0u8; 8];
//         self.0.read_exact(&mut buf)?;
//         let timestamp = Utc.timestamp_nanos(i64::from_le_bytes(buf));
//
//         Ok(Index {
//             file_id,
//             pos,
//             len,
//             timestamp,
//         })
//     }
// }
//
// impl<R: AsyncReadRentAt + AsyncReadRentExt> Iterator for StreamReader<R, Command> {
//     type Item = Result<Command>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         let prev_pos = self.0.pos;
//
//         let result = self.read_cmd(prev_pos);
//         if result.is_err() {
//             self.0.pos = prev_pos;
//         }
//
//         match result {
//             Err(anyhow::Error { .. }) => None,
//             Ok(x) => Some(Ok(x)),
//         }
//     }
// }
//
// impl<R: AsyncReadRentAt + AsyncReadRentExt> Iterator for StreamReader<R, Index> {
//     type Item = Result<Index>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }
//
// impl<R: AsyncReadRentAt + AsyncReadRentExt> BufReaderWithPos<R> {
//     pub fn into_iter<T>(self) -> StreamReader<R, T> {
//         StreamReader(self, PhantomData::<T>)
//     }
// }
