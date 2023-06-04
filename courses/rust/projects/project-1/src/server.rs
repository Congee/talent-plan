use bytes::Bytes;
use tonic::{Request, Response, Status};

use message::kv_server::{Kv, KvServer};

pub mod message {
    tonic::include_proto!("kv");
}

use crate::KvStore;

pub struct StoreServer(KvStore);

#[tonic::async_trait]
impl Kv for StoreServer {
    async fn get(
        &self,
        req: Request<message::GetReq>,
    ) -> Result<Response<message::GetRep>, Status> {
        // TODO: remove the conversion https://github.com/hyperium/tonic/issues/908
        let result = match self.0.get(Bytes::from(req.get_ref().key.clone())).await {
            Ok(_) => Some(message::get_rep::Result::Value("hello".into())),
            Err(err) => Some(message::get_rep::Result::Error(err.to_string())),
        };

        Ok(Response::new(message::GetRep { result }))
    }

    async fn set(
        &self,
        req: Request<message::SetReq>,
    ) -> Result<Response<message::SetRep>, Status> {
        let r = req.get_ref();

        let result = match self
            .0
            .set(Bytes::from(r.key.clone()), Bytes::from(r.value.clone()))
            .await
        {
            Ok(_) => Some(message::set_rep::Result::Ok(true)),
            Err(err) => Some(message::set_rep::Result::Error(err.to_string())),
        };

        Ok(Response::new(message::SetRep { result }))
    }

    async fn del(
        &self,
        req: Request<message::DelReq>,
    ) -> Result<Response<message::DelRep>, Status> {
        let result = match self.0.del(Bytes::from(req.get_ref().key.clone())).await {
            Ok(_) => Some(message::del_rep::Result::Ok(true)),
            Err(err) => Some(message::del_rep::Result::Error(err.to_string())),
        };

        Ok(Response::new(message::DelRep { result }))
    }
}

pub async fn run_server(kv_store: KvStore) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:9000".parse()?;
    let server = tonic::transport::Server::builder()
        .executor(crate::executor::GlommioExec)
        .add_service(KvServer::new(StoreServer(kv_store)));

    server.serve(addr).await?;

    Ok(())
}

// pub async fn run_server(
//     addr: SocketAddr,
//     tx: flume::Sender<StoreReq>,
//     rx: flume::Receiver<StoreRep>,
// ) -> crate::Result<()> {
//     use tarpc::server::{BaseChannel, Channel};
//     use tarpc::tokio_serde::formats::Bincode;
//     use tarpc::tokio_util::codec::length_delimited::LengthDelimitedCodec;
//     use glommio::net::TcpListener;
//
//     let listener = TcpListener::bind(addr).expect(&format!("unable to bind to {:?}", addr));
//     let codec_builer = LengthDelimitedCodec::builder();
//     let service = crate::server::Server::new(tx, rx).serve();
//
//     tokio_uring::spawn(async move {
//         let conn = listener.accept().await.unwrap();
//         // TODO: monoio with AsyncRead + AsyncWrite?
//         let tp = tarpc::serde_transport::new(codec_builer.new_framed(conn), Bincode::default());
//
//         let fut = BaseChannel::with_defaults(tp).execute(service);
//         tokio_uring::spawn(fut)
//     });
//
//     Ok(())
// }
