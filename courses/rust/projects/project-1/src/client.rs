use tonic::transport::Channel;

pub mod message {
    tonic::include_proto!("kv");
}

pub async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    let svc = Channel::from_static("http://[::1]:9000")
        .executor(crate::executor::GlommioExec)
        .connect()
        .await?;

    let channel = tower::ServiceBuilder::new().service(svc);
    let mut client = message::kv_client::KvClient::new(channel);

    client
        .get(message::GetReq {
            key: "hello".into(),
        })
        .await?;

    Ok(())
}
