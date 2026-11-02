use crate::{
    config::Scheduler,
    error::{Error, Result},
    model::*,
    recipe::digest,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{path::PathBuf, time::Duration};

#[derive(Clone)]
pub struct State {
    pub(crate) path: PathBuf,
}
impl State {
    pub async fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Signed input URLs can be persisted for retries. New databases are private.
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        let state = Self {
            path: tokio::fs::canonicalize(path).await?,
        };
        state.call(|c| {
            let version: i64 = c.pragma_query_value(None, "user_version", |r| r.get(0))?;
            if version > 2 { return Err(Error::Config("database schema is newer than this binary".into())); }
            c.pragma_update(None, "journal_mode", "WAL")?;
            c.execute_batch("CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, dedup TEXT NOT NULL, task TEXT NOT NULL,
                status TEXT NOT NULL, priority INTEGER NOT NULL, lease_ms INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, created_ms INTEGER NOT NULL,
                updated_ms INTEGER NOT NULL, available_ms INTEGER NOT NULL,
                artifact TEXT, error TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS active_dedup ON jobs(dedup) WHERE status IN ('queued','generating');
            CREATE INDEX IF NOT EXISTS pending_jobs ON jobs(status,priority,available_ms,created_ms);
            CREATE TABLE IF NOT EXISTS scheduler_activity (
                id INTEGER PRIMARY KEY CHECK (id=1), last_interactive_ms INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO scheduler_activity VALUES (1,0);
            PRAGMA user_version = 2;")?;
            Ok(())
        }).await?;
        Ok(state)
    }
    pub(crate) async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = Connection::open(path)?;
            c.busy_timeout(Duration::from_secs(5))?;
            f(&mut c)
        })
        .await
        .map_err(|_| Error::Operation("database worker stopped".into()))?
    }
    pub async fn submit(&self, task: Task, priority: Priority, policy: Scheduler) -> Result<Job> {
        self.call(move |c| {
            let now = now_ms();
            let data = serde_json::to_string(&task)?;
            let key = digest(data.as_bytes());
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if priority != Priority::Background {
                tx.execute("UPDATE scheduler_activity SET last_interactive_ms=MAX(last_interactive_ms,?) WHERE id=1", [now])?;
            }
            let existing: Option<String> = tx.query_row("SELECT id FROM jobs WHERE dedup=? AND status IN ('queued','generating')", [&key], |r| r.get(0)).optional()?;
            let id = if let Some(id) = existing {
                tx.execute("UPDATE jobs SET priority=MIN(priority,?1), lease_ms=MAX(lease_ms,?2), updated_ms=?3 WHERE id=?4 AND status='queued'", params![priority.rank(), if priority==Priority::Visible { now + millis(policy.visible_lease_seconds) } else { 0 }, now, id])?;
                id
            } else {
                let count: i64 = tx.query_row("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','generating')", [], |r| r.get(0))?;
                if count >= policy.queue_capacity as i64 { return Err(Error::QueueFull); }
                let id = uuid::Uuid::new_v4().to_string();
                tx.execute("INSERT INTO jobs(id,dedup,task,status,priority,lease_ms,created_ms,updated_ms,available_ms) VALUES (?1,?2,?3,'queued',?4,?5,?6,?6,?6)", params![id,key,data,priority.rank(),if priority==Priority::Visible { now+millis(policy.visible_lease_seconds) } else {0},now])?;
                id
            };
            tx.commit()?;
            read(c, &id)
        }).await
    }
    pub async fn get(&self, id: &str) -> Result<Job> {
        let id = id.to_owned();
        self.call(move |c| read(c, &id)).await
    }
    pub async fn list(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        self.call(move |c| {
            let mut s =
                c.prepare("SELECT id FROM jobs ORDER BY created_ms DESC,id DESC LIMIT ? OFFSET ?")?;
            let ids = s
                .query_map(params![limit.min(1000) as i64, offset as i64], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ids.iter().map(|id| read(c, id)).collect()
        })
        .await
    }
    pub async fn cancel(&self, id: &str) -> Result<Job> {
        let id = id.to_owned();
        self.call(move |c| {
            c.execute("UPDATE jobs SET status='cancelled',updated_ms=?,error=NULL WHERE id=? AND status IN ('queued','generating')", params![now_ms(),id])?;
            read(c,&id)
        }).await
    }
    pub async fn prioritize(
        &self,
        id: &str,
        priority: Priority,
        lease_seconds: u64,
    ) -> Result<Job> {
        let id = id.to_owned();
        self.call(move |c| {
            let now = now_ms();
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute("UPDATE scheduler_activity SET last_interactive_ms=MAX(last_interactive_ms,?) WHERE id=1", [now])?;
            tx.execute(
                "UPDATE jobs SET priority=?,lease_ms=?,updated_ms=? WHERE id=? AND status='queued'",
                params![
                    priority.rank(),
                    if priority == Priority::Visible {
                        now + millis(lease_seconds)
                    } else {
                        0
                    },
                    now,
                    id
                ],
            )?;
            tx.commit()?;
            read(c, &id)
        })
        .await
    }
    pub(crate) async fn recover(&self) -> Result<usize> {
        self.call(|c| Ok(c.execute("UPDATE jobs SET status='queued',attempts=MAX(0,attempts-1),available_ms=?1,updated_ms=?1,error=NULL WHERE status='generating'", [now_ms()])?)).await
    }
    pub async fn mark_interactive(&self) -> Result<()> {
        self.call(|c| { c.execute("UPDATE scheduler_activity SET last_interactive_ms=MAX(last_interactive_ms,?) WHERE id=1", [now_ms()])?; Ok(()) }).await
    }
    pub(crate) async fn claim(
        &self,
        policy: Scheduler,
        background_allowed: bool,
    ) -> Result<Option<Claimed>> {
        self.call(move |c| {
            let now=now_ms(); let tx=c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let last_interactive: i64 = tx.query_row("SELECT last_interactive_ms FROM scheduler_activity WHERE id=1", [], |r|r.get(0))?;
            let background_allowed = background_allowed && now.saturating_sub(last_interactive) >= millis(policy.background_idle_seconds);
            tx.execute("UPDATE jobs SET priority=1 WHERE status='queued' AND priority=0 AND lease_ms<=?",[now])?;
            tx.execute("UPDATE jobs SET priority=2 WHERE id IN (SELECT id FROM jobs WHERE status='queued' AND priority=1 ORDER BY created_ms,id LIMIT -1 OFFSET ?)",[policy.prefetch_limit as i64])?;
            let found: Option<(String,String)> = tx.query_row("SELECT id,task FROM jobs WHERE status='queued' AND available_ms<=?1 AND (?2 OR priority<2) ORDER BY priority,created_ms,id LIMIT 1", params![now,background_allowed], |r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let Some((id,data))=found else { tx.commit()?; return Ok(None); };
            tx.execute("UPDATE jobs SET status='generating',attempts=attempts+1,updated_ms=?,error=NULL WHERE id=?", params![now,id])?;
            tx.commit()?;
            Ok(Some(Claimed { job: read(c,&id)?, task:serde_json::from_str(&data)? }))
        }).await
    }
    pub(crate) async fn complete(&self, id: &str, artifact: Artifact) -> Result<()> {
        let id = id.to_owned();
        self.call(move |c| {
            c.execute("UPDATE jobs SET status='ready',artifact=?,updated_ms=?,error=NULL WHERE id=? AND status='generating'", params![serde_json::to_string(&artifact)?,now_ms(),id])?; Ok(())
        }).await
    }
    pub(crate) async fn fail(
        &self,
        id: &str,
        error: String,
        policy: Scheduler,
        retryable: bool,
    ) -> Result<()> {
        let id = id.to_owned();
        self.call(move |c| {
            let tx=c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let attempts:u32=tx.query_row("SELECT attempts FROM jobs WHERE id=?",[&id],|r|r.get(0))?;
            let retry=retryable && attempts<policy.max_attempts;
            let delay=policy.retry_initial_seconds.saturating_mul(2u64.saturating_pow(attempts.saturating_sub(1))).min(policy.retry_max_seconds);
            let now=now_ms();
            tx.execute("UPDATE jobs SET status=?,error=?,updated_ms=?,available_ms=? WHERE id=? AND status='generating'",params![if retry {"queued"} else {"error"},error,now,now.saturating_add(millis(delay)),id])?;
            tx.commit()?; Ok(())
        }).await
    }
    pub(crate) async fn interrupt(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.call(move|c| {c.execute("UPDATE jobs SET status='queued',attempts=MAX(0,attempts-1),updated_ms=?1,available_ms=?1 WHERE id=?2 AND status='generating'",params![now_ms(),id])?;Ok(())}).await
    }
}
fn millis(seconds: u64) -> i64 {
    seconds.saturating_mul(1000).min(i64::MAX as u64 / 2) as i64
}
fn read(c: &Connection, id: &str) -> Result<Job> {
    let raw=c.query_row("SELECT status,priority,attempts,created_ms,updated_ms,task,artifact,error FROM jobs WHERE id=?",[id],|r| Ok((r.get::<_,String>(0)?,r.get::<_,i32>(1)?,r.get::<_,u32>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,String>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,Option<String>>(7)?))).optional()?.ok_or(Error::NotFound)?;
    let status = match raw.0.as_str() {
        "queued" => Status::Queued,
        "generating" => Status::Generating,
        "ready" => Status::Ready,
        "error" => Status::Error,
        "cancelled" => Status::Cancelled,
        _ => return Err(Error::Operation("unknown persisted job status".into())),
    };
    Ok(Job {
        id: id.into(),
        status,
        priority: match raw.1 {
            0 => Priority::Visible,
            1 => Priority::Prefetch,
            _ => Priority::Background,
        },
        attempts: raw.2,
        created_ms: raw.3,
        updated_ms: raw.4,
        recipe: serde_json::from_str::<Task>(&raw.5)?.recipe,
        artifact: raw.6.map(|s| serde_json::from_str(&s)).transpose()?,
        error: raw.7,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn task(name: &str) -> Task {
        Task {
            input: Input::Local { path: name.into() },
            recipe: Default::default(),
            force: false,
        }
    }
    #[tokio::test]
    async fn schema_one_migration_preserves_jobs_and_rejects_future_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.sqlite");
        let state = State::open(path.clone()).await.unwrap();
        let job = state
            .submit(task("preserved"), Priority::Prefetch, Scheduler::default())
            .await
            .unwrap();
        state
            .call(|c| {
                c.execute_batch("DROP TABLE scheduler_activity; PRAGMA user_version=1;")?;
                Ok(())
            })
            .await
            .unwrap();
        let migrated = State::open(path.clone()).await.unwrap();
        assert_eq!(migrated.get(&job.id).await.unwrap().status, Status::Queued);
        assert_eq!(migrated.list(10, 0).await.unwrap().len(), 1);
        let version: i64 = migrated
            .call(|c| Ok(c.pragma_query_value(None, "user_version", |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(version, 2);
        migrated
            .call(|c| {
                c.pragma_update(None, "user_version", 3)?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(State::open(path).await, Err(Error::Config(_))));
    }
    #[tokio::test]
    async fn independent_clients_share_interactive_activity_and_priority_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("jobs.sqlite");
        let owner = State::open(path.clone()).await.unwrap();
        let client = State::open(path).await.unwrap();
        let policy = Scheduler {
            background_idle_seconds: 60,
            ..Default::default()
        };
        let background = client
            .submit(task("background"), Priority::Background, policy.clone())
            .await
            .unwrap();
        client
            .submit(task("foreground"), Priority::Prefetch, policy.clone())
            .await
            .unwrap();
        assert_eq!(
            owner
                .claim(policy.clone(), true)
                .await
                .unwrap()
                .unwrap()
                .job
                .priority,
            Priority::Prefetch
        );
        assert!(owner.claim(policy.clone(), true).await.unwrap().is_none());
        // A priority update through another connection also resets the shared clock.
        owner
            .call(|c| {
                c.execute("UPDATE scheduler_activity SET last_interactive_ms=0", [])?;
                Ok(())
            })
            .await
            .unwrap();
        client
            .prioritize(
                &background.id,
                Priority::Background,
                policy.visible_lease_seconds,
            )
            .await
            .unwrap();
        assert!(owner.claim(policy.clone(), true).await.unwrap().is_none());
        let immediate = Scheduler {
            background_idle_seconds: 0,
            ..policy
        };
        assert_eq!(
            owner.claim(immediate, true).await.unwrap().unwrap().job.id,
            background.id
        );
    }
    #[tokio::test]
    async fn concurrent_submit_claim_cancel_and_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let db = State::open(tmp.path().join("jobs.sqlite")).await.unwrap();
        let policy = Scheduler::default();
        let mut joins = vec![];
        for _ in 0..16 {
            let db = db.clone();
            let p = policy.clone();
            joins.push(tokio::spawn(async move {
                db.submit(task("same"), Priority::Prefetch, p)
                    .await
                    .unwrap()
            }));
        }
        let id = joins.remove(0).await.unwrap().id;
        for join in joins {
            assert_eq!(join.await.unwrap().id, id);
        }
        assert_eq!(db.list(100, 0).await.unwrap().len(), 1);
        let visible = db
            .submit(task("visible"), Priority::Visible, policy.clone())
            .await
            .unwrap();
        let first = db.claim(policy.clone(), true).await.unwrap().unwrap();
        assert_eq!(first.job.id, visible.id);
        db.cancel(&visible.id).await.unwrap();
        db.complete(
            &visible.id,
            Artifact {
                key: "x".into(),
                size: 1,
                cache_hit: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(db.get(&visible.id).await.unwrap().status, Status::Cancelled);
        let claimed = db.claim(policy.clone(), true).await.unwrap().unwrap();
        assert_eq!(claimed.job.id, id);
        assert!(db.claim(policy.clone(), true).await.unwrap().is_none());
        assert_eq!(db.recover().await.unwrap(), 1);
        assert_eq!(db.get(&id).await.unwrap().attempts, 0);
        assert_eq!(db.claim(policy, true).await.unwrap().unwrap().job.id, id);
    }
    #[tokio::test]
    async fn capacity_background_gate_and_retry_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let db = State::open(tmp.path().join("jobs.sqlite")).await.unwrap();
        let p = Scheduler {
            queue_capacity: 1,
            max_attempts: 1,
            ..Default::default()
        };
        let job = db
            .submit(task("background"), Priority::Background, p.clone())
            .await
            .unwrap();
        assert!(matches!(
            db.submit(task("overflow"), Priority::Visible, p.clone())
                .await,
            Err(Error::QueueFull)
        ));
        assert!(db.claim(p.clone(), false).await.unwrap().is_none());
        db.claim(p.clone(), true).await.unwrap().unwrap();
        db.fail(&job.id, "test error".into(), p.clone(), true)
            .await
            .unwrap();
        assert_eq!(db.get(&job.id).await.unwrap().status, Status::Error);
        let replacement = db
            .submit(task("background"), Priority::Prefetch, p)
            .await
            .unwrap();
        assert_ne!(job.id, replacement.id);
    }
}
