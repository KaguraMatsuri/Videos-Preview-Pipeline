use clap::Parser;
use rust_preview_pipeline::{Config, Error, Pipeline, Result, Status, Submit};
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    request: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(Some(&args.config))?;
    let request: Submit = serde_json::from_slice(&tokio::fs::read(args.request).await?)?;
    let pipeline = Pipeline::open(config).await?;
    let workers = pipeline.start().await?;

    // Always shut down the worker owner, including when submission or polling fails.
    let result = async {
        let submitted = pipeline.submit(request).await?;
        loop {
            let job = pipeline.state.get(&submitted.id).await?;
            if job.status.terminal() {
                println!("{}", serde_json::to_string_pretty(&job)?);
                return if job.status == Status::Ready {
                    Ok(())
                } else {
                    Err(Error::Operation("job did not complete successfully".into()))
                };
            }
            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    return Err(Error::Cancelled);
                }
                _ = workers.stopped() => {
                    return Err(Error::Operation("worker owner stopped".into()));
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }
    .await;

    workers.shutdown().await?;
    result
}
