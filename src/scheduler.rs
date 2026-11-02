use crate::{
    config::Config,
    error::{Error, Result},
    model::*,
    runner,
    state::State,
    storage::Storage,
};
use std::{
    fs::File,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Pipeline {
    pub config: Arc<Config>,
    pub state: State,
    pub storage: Storage,
    wake: Arc<Notify>,
    background_paused: Arc<AtomicBool>,
}
impl Pipeline {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let config = Arc::new(config);
        let state = State::open(config.database.clone()).await?;
        let storage = Storage::new(config.clone()).await?;
        Ok(Self {
            config,
            state,
            storage,
            wake: Arc::new(Notify::new()),
            background_paused: Arc::new(AtomicBool::new(false)),
        })
    }
    pub async fn submit(&self, request: Submit) -> Result<Job> {
        let recipe = match &request.recipe {
            Some(name) => self
                .config
                .recipes
                .get(name)
                .cloned()
                .ok_or_else(|| Error::Invalid("unknown recipe".into()))?,
            None => self.config.recipe.clone(),
        }
        .overridden(&request.overrides);
        self.config.validate_recipe(&recipe)?;
        let input = self.storage.validate_input(&request.input).await?;
        let job = self
            .state
            .submit(
                Task {
                    input,
                    recipe,
                    force: request.force,
                },
                request.priority,
                self.config.scheduler.clone(),
            )
            .await?;
        self.wake.notify_waiters();
        Ok(job)
    }
    pub async fn mark_interactive(&self) -> Result<()> {
        self.state.mark_interactive().await
    }
    pub fn set_background_paused(&self, paused: bool) {
        self.background_paused.store(paused, Ordering::Relaxed);
        self.wake.notify_waiters();
    }
    pub async fn cancel(&self, id: &str) -> Result<Job> {
        let job = self.state.cancel(id).await?;
        self.wake.notify_waiters();
        Ok(job)
    }
    pub async fn prioritize(&self, id: &str, priority: Priority) -> Result<Job> {
        let job = self
            .state
            .prioritize(id, priority, self.config.scheduler.visible_lease_seconds)
            .await?;
        self.wake.notify_waiters();
        Ok(job)
    }
    pub async fn start(&self) -> Result<Workers> {
        let mut path = self.state.path.as_os_str().to_os_string();
        path.push(".worker.lock");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|_| Error::WorkerLocked)?;
        let owner = Arc::new(file);
        let recovered = self.state.recover().await?;
        self.mark_interactive().await?;
        tracing::info!(
            recovered,
            workers = self.config.scheduler.concurrency,
            "workers started"
        );
        let stop = CancellationToken::new();
        let mut handles = vec![];
        for _ in 0..self.config.scheduler.concurrency {
            let pipeline = self.clone();
            let token = stop.clone();
            let owner = owner.clone();
            handles.push(tokio::spawn(async move {
                let _owner = owner;
                // Panics and fatal persistence errors also stop the other workers.
                let _stop_on_exit = token.clone().drop_guard();
                pipeline.worker(token).await
            }));
        }
        Ok(Workers {
            stop,
            handles,
            owner,
        })
    }
    async fn worker(self, stop: CancellationToken) -> Result<()> {
        let mut background_after = tokio::time::Instant::now();
        while !stop.is_cancelled() {
            let background =
                tokio::time::Instant::now() >= background_after && self.background_allowed().await;
            let claimed = match self
                .state
                .claim(self.config.scheduler.clone(), background)
                .await
            {
                Ok(job) => job,
                Err(error) => {
                    tracing::error!(code = error.code(), "cannot claim job");
                    None
                }
            };
            let Some(claimed) = claimed else {
                tokio::select! {_=stop.cancelled()=>break,_=self.wake.notified()=>{},_=tokio::time::sleep(Duration::from_millis(self.config.scheduler.poll_ms))=>{}}
                continue;
            };
            let id = claimed.job.id.clone();
            let background = claimed.job.priority == Priority::Background;
            let cancel = stop.child_token();
            let execution = runner::execute(
                self.config.clone(),
                self.storage.clone(),
                claimed.task,
                cancel.clone(),
            );
            tokio::pin!(execution);
            let result = loop {
                tokio::select! {biased;
                    result=&mut execution=>break result,
                    _=tokio::time::sleep(Duration::from_millis(100))=>{
                        match self.state.get(&id).await {Ok(job) if job.status==Status::Cancelled=>cancel.cancel(),Err(_)=>cancel.cancel(),_=>{}}
                    }
                }
            };
            let outcome = if stop.is_cancelled() {
                Outcome::Interrupted
            } else {
                match result {
                    Ok(artifact) => Outcome::Ready(artifact),
                    Err(Error::Cancelled) => Outcome::Cancelled,
                    Err(error) => {
                        let retryable =
                            !matches!(error, Error::Invalid(_) | Error::Config(_) | Error::Json(_));
                        tracing::warn!(job_id=%id,code=error.code(),"job attempt failed");
                        Outcome::Failed {
                            message: format!("{}: {}", error.code(), error),
                            retryable,
                        }
                    }
                }
            };
            self.persist_outcome(&id, &outcome).await?;
            if background {
                background_after = tokio::time::Instant::now()
                    + Duration::from_secs(self.config.scheduler.background_cooldown_seconds);
            }
        }
        Ok(())
    }
    async fn persist_outcome(&self, id: &str, outcome: &Outcome) -> Result<()> {
        for attempt in 1..=self.config.scheduler.commit_max_attempts {
            let result = match outcome {
                Outcome::Ready(artifact) => self.state.complete(id, artifact.clone()).await,
                Outcome::Cancelled => self.state.cancel(id).await.map(|_| ()),
                Outcome::Interrupted => self.state.interrupt(id).await,
                Outcome::Failed { message, retryable } => {
                    self.state
                        .fail(
                            id,
                            message.clone(),
                            self.config.scheduler.clone(),
                            *retryable,
                        )
                        .await
                }
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::error!(job_id=%id, attempt, code=error.code(), "job state commit failed");
                    if attempt == self.config.scheduler.commit_max_attempts {
                        return Err(error);
                    }
                    tokio::time::sleep(Duration::from_secs(
                        self.config.scheduler.commit_retry_seconds,
                    ))
                    .await;
                }
            }
        }
        unreachable!("configuration requires at least one commit attempt")
    }
    async fn background_allowed(&self) -> bool {
        let s = &self.config.scheduler;
        if !s.background_enabled || self.background_paused.load(Ordering::Relaxed) {
            return false;
        }
        let min_memory = s.background_min_memory_mb.saturating_mul(1024 * 1024);
        let max_load = s.background_max_load_ratio;
        tokio::task::spawn_blocking(move || {
            let mut system = sysinfo::System::new();
            system.refresh_memory();
            let cpus = std::thread::available_parallelism().map_or(1, usize::from) as f64;
            system.available_memory() >= min_memory
                && sysinfo::System::load_average().one / cpus <= max_load
        })
        .await
        .unwrap_or(false)
    }
}
enum Outcome {
    Ready(Artifact),
    Cancelled,
    Interrupted,
    Failed { message: String, retryable: bool },
}
pub struct Workers {
    stop: CancellationToken,
    handles: Vec<JoinHandle<Result<()>>>,
    owner: Arc<File>,
}
impl Workers {
    /// Resolves when shutdown is requested or any worker fails.
    pub fn stopped(&self) -> impl std::future::Future<Output = ()> + Send + 'static + use<> {
        self.stop.clone().cancelled_owned()
    }
    pub async fn shutdown(mut self) -> Result<()> {
        self.stop.cancel();
        let mut first_error = None;
        for handle in self.handles.drain(..) {
            let result = handle
                .await
                .map_err(|_| Error::Operation("worker stopped unexpectedly".into()))
                .and_then(|result| result);
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.cancel();
        let _ = &self.owner;
    }
}
