#![allow(missing_docs)]

use std::future::Future;
use std::pin::{pin, Pin};
use std::task::Context;
use std::{async_iter::AsyncIterator, task::Poll};

use crate::fs::File;
use monoio::buf::VecBuf;

use bytes::{Buf, Bytes, BytesMut};
use chrono::{DateTime, TimeZone as _, Utc};
use crc32fast;

/// | crc | write/delete | timestamp | ksz | key | val_sz | value |
pub enum Command {
    Write {
        key: Bytes,
        value: Bytes,
        timestamp: DateTime<Utc>,
    },
    Delete {
        key: Bytes,
        timestamp: DateTime<Utc>,
    },
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl PartialOrd for Command {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Command::Write { key: lhs, .. }, Command::Write { key: rhs, .. }) => {
                lhs.partial_cmp(rhs)
            }
            (Command::Delete { key: lhs, .. }, Command::Delete { key: rhs, .. }) => {
                lhs.partial_cmp(rhs)
            }
            _ => None,
        }
    }
}

impl Command {
    /// move key & value because io_uring requires a non-volatile buffer to copy
    pub async fn to_file(self, file: &mut File) -> std::io::Result<usize> {
        // | crc | write/delete | timestamp | ksz | value_sz | key | value |
        // | u32 | u8           | i64       | usz | usz      | _   | _     |
        let iovec = match self {
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
                    Bytes::copy_from_slice(&crc32.to_le_bytes()),
                    Bytes::copy_from_slice(&(0 as u8).to_le_bytes()),
                    Bytes::copy_from_slice(&timestamp.timestamp_nanos().to_le_bytes()),
                    Bytes::copy_from_slice(&(key.len() as u64).to_le_bytes()),
                    Bytes::copy_from_slice(&(value.len() as u64).to_le_bytes()),
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
                    Bytes::copy_from_slice(&crc32.to_le_bytes()),
                    Bytes::copy_from_slice(&(1 as u8).to_le_bytes()),
                    Bytes::copy_from_slice(&timestamp.timestamp_nanos().to_le_bytes()),
                    Bytes::copy_from_slice(&(key.len() as u64).to_le_bytes()),
                    key,
                ]
            }
        };

        file.append(VecBuf::from(iovec)).await
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

        let iovec = vec![
            Vec::from(self.file_id.to_le_bytes()),
            Vec::from(self.pos.to_le_bytes()),
            Vec::from(self.len.to_le_bytes()),
            Vec::from(self.timestamp.timestamp_nanos().to_le_bytes()),
        ];

        writer.append(VecBuf::from(iovec)).await
    }
}

pub struct StreamReader<'a, T> {
    pub cursor: u64,
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
}

impl<'a> StreamReader<'a, Command> {
    pub(crate) async fn read_entry(&mut self) -> std::io::Result<Option<Command>> {
        // | crc | write/delete | timestamp | ksz | value_sz | key | value |
        // | u32 | u8           | i64       | usz | usz      | _   | _     |
        let vecbuf = VecBuf::from(vec![
            BytesMut::with_capacity(4),
            BytesMut::with_capacity(1),
            BytesMut::with_capacity(8),
            BytesMut::with_capacity(8),
        ]);
        let mut pos = self.cursor;
        let (result, buf) = self.file.preadv_exact(vecbuf, pos).await;
        if let Err(err) = &result {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            } else {
                panic!()
            }
        }

        let mut iovec: Vec<BytesMut> = buf.into();
        let _crc32 = iovec[0].get_u32_le();
        let is_write = iovec[1][0] == 0;
        let ts = iovec[2].get_i64_le();
        let timestamp = Utc.timestamp_nanos(ts);
        let ksz = iovec[3].get_u64_le();

        pos += 4 + 1 + 8 + 8;

        if is_write {
            let (result, vszbuf) = self.file.pread_exact(Box::new([0; 8]), pos).await;
            result?;
            let vsz = u64::from_le_bytes(Box::into_inner(vszbuf));
            pos += 8;

            let vecbuf = VecBuf::from(vec![
                BytesMut::with_capacity(ksz as _),
                BytesMut::with_capacity(vsz as _),
            ]);
            let (result, vecbuf) = self.file.preadv_exact(vecbuf, pos).await;
            result?;

            self.cursor = pos + (ksz + vsz) as u64;
            let iovec: Vec<BytesMut> = vecbuf.into();

            Ok(Some(Command::Write {
                key: iovec[0].clone().freeze(),
                value: iovec[1].clone().freeze(),
                timestamp,
            }))
        } else {
            let key = BytesMut::with_capacity(ksz as _);
            let (result, key) = self.file.pread_exact(key, pos).await;
            result?;
            self.cursor = pos + ksz as u64;

            Ok(Some(Command::Delete {
                key: key.freeze(),
                timestamp,
            }))
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
