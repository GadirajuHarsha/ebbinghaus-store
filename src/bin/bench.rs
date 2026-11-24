#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    let args: Vec<String> = std::env::args().collect();
    ebbinghaus_store::bench::run_benchmark(&args).await
}

