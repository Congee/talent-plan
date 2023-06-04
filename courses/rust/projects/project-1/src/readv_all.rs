// use libc::iovec;
// use std::io;
// use tokio_uring::buf::BoundedBuf;
// use tokio_uring::io::SharedFd;
// use tokio_uring::runtime::driver::op::{Completable, CqeResult, Op};
// use tokio_uring::runtime::CONTEXT;
//
// pub async fn readv_at_all<T: BoundedBuf>(
//     fd: &SharedFd,
//     mut bufs: Vec<T>,
//     offset: Option<u64>,
// ) -> tokio_uring::BufResult<usize, Vec<T>> {
//     let mut fd = fd.clone();
//
//     let mut iovs: Vec<iovec> = bufs
//         .iter_mut()
//         .map(|b| iovec {
//             iov_base: b.stable_ptr() as *mut libc::c_void,
//             iov_len: b.bytes_init(),
//         })
//         .collect();
//
//     let mut iovs_ptr = iovs.as_ptr();
//     let mut iovs_len = iovs.len() as u32;
//
//     let mut nread: usize = 0;
//
//     loop {
//         let o = match offset {
//             Some(m) => m + (nread as u64),
//             None => 0,
//         };
//
//         let op = MyOp::readv_at_all2(fd, bufs, iovs, iovs_ptr, iovs_len, o).unwrap();
//         let res;
//         (res, fd, bufs, iovs) = op.await;
//
//         let mut n: usize = match res {
//             Ok(m) => m,
//             Err(e) => return (Err(e), bufs),
//         };
//
//         nread += n;
//
//         // Consume n and iovs_len until one or the other is exhausted.
//         while n != 0 && iovs_len > 0 {
//             // safety: iovs_len > 0, so safe to dereference the const *.
//             let mut iovec = unsafe { *iovs_ptr };
//             let iov_len = iovec.iov_len;
//             if n >= iov_len {
//                 n -= iov_len;
//                 // safety: iovs_len > 0, so safe to add 1 as iovs_len is decremented by 1.
//                 iovs_ptr = unsafe { iovs_ptr.add(1) };
//                 iovs_len -= 1;
//             } else {
//                 // safety: n was found to be less than iov_len, so adding to base and keeping
//                 // iov_len updated by decrementing maintains the invariant of the iovec
//                 // representing how much of the buffer remains to be written to.
//                 iovec.iov_base = unsafe { (iovec.iov_base as *const u8).add(n) } as _;
//                 iovec.iov_len -= n;
//                 n = 0;
//             }
//         }
//
//         // Assert that both n and iovs_len become exhausted simultaneously.
//
//         if (iovs_len == 0 && n != 0) || (iovs_len > 0 && n == 0) {
//             unreachable!();
//         }
//
//         // We are done when n and iovs_len have been consumed.
//         if n == 0 {
//             break;
//         }
//     }
//
//     (Ok(nread), bufs)
// }
//
// struct ReadvAll<T> {
//     /// Holds a strong ref to the FD, preventing the file from being closed
//     /// while the operation is in-flight.
//     fd: SharedFd,
//
//     bufs: Vec<T>,
//
//     iovs: Vec<iovec>,
// }
//
// struct MyOp<T>(T);
//
// impl<T: BoundedBuf> MyOp<ReadvAll<T>> {
//     fn readv_at_all2(
//         // Three values to share to keep live.
//         fd: SharedFd,
//         bufs: Vec<T>,
//         iovs: Vec<iovec>,
//
//         // Three values to use for this invocation.
//         iovs_ptr: *const iovec,
//         iovs_len: u32,
//         offset: u64,
//     ) -> io::Result<Op<ReadvAll<T>>> {
//         use tokio_uring::io_uring::{opcode, types};
//
//         CONTEXT.with(|x| {
//             x.handle().expect("Not in a runtime context").submit_op(
//                 ReadvAll { fd, bufs, iovs },
//                 // So this wouldn't need to be a function. Just pass in the entry.
//                 |read| {
//                     opcode::Readv::new(types::Fd(read.fd.raw_fd()), iovs_ptr, iovs_len)
//                         .offset(offset as _)
//                         .build()
//                 },
//             )
//         })
//     }
// }
//
// impl<T> Completable for ReadvAll<T>
// where
//     T: BoundedBuf,
// {
//     type Output = (Result<usize, io::Error>, SharedFd, Vec<T>, Vec<iovec>);
//
//     fn complete(self, cqe: CqeResult) -> Self::Output {
//         // Convert the operation result to `usize`
//         let res = cqe.result.map(|v| v as usize);
//
//         (res, self.fd, self.bufs, self.iovs)
//     }
// }
