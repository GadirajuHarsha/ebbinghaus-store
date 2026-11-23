fn main() -> Result<(), Box<dyn std::error::Error>>
{
    // compile protobuf definitions into rust code
    tonic_build::compile_protos("proto/ebbinghaus.proto")?;
    Ok(())
}

