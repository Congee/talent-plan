use tokio_uring::buf::BoundedBuf;
use tokio_uring::buf::IoBuf;
use tokio_uring::BufResult;

pub struct File {
    pub pos: usize,
    file: tokio_uring::fs::File,
    pub path: std::path::PathBuf,
    // total_size: u64,
    // data_size: u64,
}

impl File {
    pub fn new(path: std::path::PathBuf, file: tokio_uring::fs::File) -> Self {
        Self {
            pos: 0,
            file,
            path,
            // data_size: 0,
            // total_size: 0,
        }
    }

    pub fn inner(&self) -> &tokio_uring::fs::File {
        &self.file
    }

    pub async fn append<T: IoBuf>(&mut self, buf: Vec<T>) -> std::io::Result<usize> {
        self.file
            .writev_at_all(buf, Some(self.pos as _))
            .await
            .0
            .map(|nwritten| {
                self.pos += nwritten;
                nwritten
            })
    }

    pub async fn readv_at_all<T: BoundedBuf>(
        &self,
        buf: Vec<T>,
        pos: Option<u64>,
    ) -> BufResult<usize, Vec<T>> {
        crate::readv_all::readv_at_all(&self.file.fd, buf, pos).await
    }
}
