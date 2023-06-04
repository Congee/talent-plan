use monoio::buf::{IoBufMut, IoVecBufMut};
use monoio::driver::op::Op;
use monoio::{buf::IoVecBuf, BufResult};

pub struct File {
    pub pos: usize,
    file: monoio::fs::File,
    pub path: std::path::PathBuf,
    total_size: u64,
    data_size: u64,
}

impl File {
    pub fn new(path: std::path::PathBuf, file: monoio::fs::File) -> Self {
        Self { pos: 0, file , path, data_size: 0, total_size: 0}
    }

    pub fn inner(&self) -> &monoio::fs::File {
        &self.file
    }

    async fn writev<T: IoVecBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let len = buf.read_iovec_len();
        let op = Op::writev(&self.file.fd, buf).unwrap();
        let (res, slice) = op.write().await;
        let _ = res.as_ref().map(|_| self.pos += len);
        (res, slice)
    }

    pub async fn append<T: IoVecBuf>(&mut self, buf: T) -> std::io::Result<usize> {
        self.writev(buf).await.0
    }

    pub async fn pread_exact<T: IoBufMut>(&self, buf: T, pos: u64) -> BufResult<(), T> {
        // Box::pin(self);
        self.file.read_exact_at(buf, pos).await
    }

    async fn readv<T: IoVecBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        let op = Op::readv(self.file.fd.clone(), buf).unwrap();
        op.read().await
    }

    pub async fn readv_exact<T: IoVecBufMut>(&mut self, mut buf: T) -> BufResult<usize, T> {
        // copied from
        //     monoio/monoio/src/io/async_read_rent_ext.rs
        let mut meta = monoio::buf::write_vec_meta(&mut buf);
        let len = meta.len();
        let mut read = 0;
        while read < len {
            let (res, meta_) = self.readv(meta).await;
            meta = meta_;
            match res {
                Ok(0) => {
                    return (
                        Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        )),
                        buf,
                    )
                }
                Ok(n) => read += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
        (Ok(read), buf)
    }

    async fn preadv<T: IoVecBufMut>(&self, buf: T, pos: u64) -> BufResult<usize, T> {
        let op = Op::preadv(self.file.fd.clone(), buf, pos).unwrap();
        op.read().await
    }

    pub async fn preadv_exact<T: IoVecBufMut>(
        &self,
        mut buf: T,
        pos: u64,
    ) -> BufResult<usize, T> {
        // copied from
        //     monoio/monoio/src/io/async_read_rent_ext.rs
        let mut meta = monoio::buf::write_vec_meta(&mut buf);
        let len = meta.len();
        let mut read = 0;
        while read < len {
            let (res, meta_) = self.preadv(meta, pos + read as u64).await;
            meta = meta_;
            match res {
                Ok(0) => {
                    return (
                        Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        )),
                        buf,
                    )
                }
                Ok(n) => read += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
            dbg!("before preadv", read, len);
        }
        (Ok(read), buf)
    }
}

// impl AsRef<Reader> for Reader {
//     fn as_ref(&self) -> &Reader {
//         &self
//     }
// }
//
// #[allow(dead_code)]
// struct BufReader {
//     pos: u64,
//     reader: Reader,
//     buf: Box<[u8]>,
//     acc: usize,
// }
//
// #[allow(dead_code)]
// impl BufReader {
//     pub fn new(file: monoio::fs::File, buf_size: usize) -> Self {
//         Self {
//             pos: 0,
//             reader: Reader::new(file),
//             buf: vec![0u8; buf_size.max(512)].into_boxed_slice(),
//             acc: 0,
//         }
//     }
// }
