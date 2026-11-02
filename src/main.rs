use clap::{Parser, Subcommand, ValueEnum};
use rust_preview_pipeline::{Config, Error, Pipeline, Priority, Result, Status, Submit};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Configurable video-to-GIF pipeline")]
struct Cli {
    /// TOML configuration. Environment overrides use PREVIEW__SECTION__FIELD.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    /// Validate configuration without opening the database or starting workers.
    Validate,
    /// Submit a JSON job, start workers, and wait for this job to finish.
    Run {
        #[arg(long)]
        request: PathBuf,
    },
    /// Submit a JSON job for an already running or future worker process.
    Submit {
        #[arg(long)]
        request: PathBuf,
    },
    /// Run persistent workers until interrupted.
    Worker,
    /// Run the HTTP API and workers until interrupted.
    #[cfg(feature = "http")]
    Serve,
    /// Read one job's state as JSON.
    Status { id: String },
    /// List jobs, newest first, as JSON.
    List {
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long, default_value_t = 0)]
        offset: usize,
    },
    /// Cancel a queued or running job.
    Cancel { id: String },
    /// Change a queued job's priority and renew its visible lease.
    Prioritize {
        id: String,
        #[arg(value_enum)]
        priority: CliPriority,
    },
}
#[derive(Clone, ValueEnum)]
enum CliPriority {
    Visible,
    Prefetch,
    Background,
}
impl From<CliPriority> for Priority {
    fn from(p: CliPriority) -> Self {
        match p {
            CliPriority::Visible => Self::Visible,
            CliPriority::Prefetch => Self::Prefetch,
            CliPriority::Background => Self::Background,
        }
    }
}
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!(
            "{}",
            serde_json::json!({"code":e.code(),"message":e.to_string()})
        );
        std::process::exit(1);
    }
}
async fn request(path: PathBuf) -> Result<Submit> {
    let bytes = tokio::fs::read(path).await?;
    if bytes.len() > 65536 {
        return Err(Error::Invalid("request file exceeds 64 KiB".into()));
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn print(value: impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(cli.config.as_deref())?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&config.log_level)
                .map_err(|_| Error::Config("invalid log_level".into()))?,
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    if matches!(cli.command, Action::Validate) {
        return print(serde_json::json!({"valid":true}));
    }
    let p = Pipeline::open(config).await?;
    match cli.command {
        Action::Validate => unreachable!(),
        Action::Submit { request: path } => print(p.submit(request(path).await?).await?),
        Action::Run { request: path } => {
            let job = p.submit(request(path).await?).await?;
            let workers = p.start().await?;
            let result=async{loop{
                let job=p.state.get(&job.id).await?;
                if job.status.terminal(){print(&job)?;return if job.status==Status::Ready{Ok(())}else{Err(Error::Operation("job did not complete successfully".into()))};}
                tokio::select!{_=signal()=>return Err(Error::Cancelled),_=workers.stopped()=>return Err(Error::Operation("worker owner stopped".into())),_=tokio::time::sleep(std::time::Duration::from_millis(100))=>{}}
            }}.await;
            workers.shutdown().await?;
            result
        }
        Action::Worker => {
            let workers = p.start().await?;
            tokio::select! { _=signal()=>{}, _=workers.stopped()=>{} }
            workers.shutdown().await
        }
        #[cfg(feature = "http")]
        Action::Serve => {
            let router = rust_preview_pipeline::http::router(p.clone())?;
            let listener = tokio::net::TcpListener::bind(p.config.server.listen).await?;
            let workers = p.start().await?;
            tracing::info!(address=%listener.local_addr()?,"HTTP server listening");
            let stopped = workers.stopped();
            let result = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    tokio::select! { _=signal()=>{}, _=stopped=>{} }
                })
                .await;
            workers.shutdown().await?;
            result.map_err(Error::Io)
        }
        Action::Status { id } => print(p.state.get(&id).await?),
        Action::List { limit, offset } => print(p.state.list(limit, offset).await?),
        Action::Cancel { id } => print(p.cancel(&id).await?),
        Action::Prioritize { id, priority } => print(p.prioritize(&id, priority.into()).await?),
    }
}
async fn signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
