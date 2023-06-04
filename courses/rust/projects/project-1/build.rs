fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();

    config.bytes(&["."]);

    tonic_build::compile_protos("proto/message.proto")?;
    Ok(())
}
