use crate::{
    error::{Error, Result},
    recipe::Recipe,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub database: PathBuf,
    pub temp_dir: PathBuf,
    pub recipe: Recipe,
    pub recipes: BTreeMap<String, Recipe>,
    pub execution: Execution,
    pub scheduler: Scheduler,
    pub cache: Cache,
    pub inputs: Inputs,
    pub output: Output,
    pub stores: BTreeMap<String, S3Config>,
    pub server: Server,
    pub log_level: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            database: ".pipeline/jobs.sqlite".into(),
            temp_dir: ".pipeline/tmp".into(),
            recipe: Recipe::default(),
            recipes: BTreeMap::new(),
            execution: Execution::default(),
            scheduler: Scheduler::default(),
            cache: Cache::default(),
            inputs: Inputs::default(),
            output: Output::default(),
            stores: BTreeMap::new(),
            server: Server::default(),
            log_level: "info".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Execution {
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
    pub threads: u16,
    pub timeout_seconds: u64,
    pub io_timeout_seconds: u64,
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub max_duration_seconds: f64,
    pub max_fps: f64,
    pub max_dimension: u32,
    pub format_whitelist: String,
}
impl Default for Execution {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".into(),
            ffprobe: "ffprobe".into(),
            threads: 1,
            timeout_seconds: 90,
            io_timeout_seconds: 120,
            max_input_bytes: 2 * 1024 * 1024 * 1024,
            max_output_bytes: 64 * 1024 * 1024,
            max_duration_seconds: 60.0,
            max_fps: 30.0,
            max_dimension: 1920,
            format_whitelist: "mov,matroska,avi,mpegts,flv,ogg".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Scheduler {
    pub concurrency: usize,
    pub queue_capacity: usize,
    pub poll_ms: u64,
    pub visible_lease_seconds: u64,
    pub prefetch_limit: usize,
    pub background_enabled: bool,
    pub background_idle_seconds: u64,
    pub background_cooldown_seconds: u64,
    pub background_max_load_ratio: f64,
    pub background_min_memory_mb: u64,
    pub max_attempts: u32,
    pub retry_initial_seconds: u64,
    pub retry_max_seconds: u64,
    pub commit_max_attempts: u32,
    pub commit_retry_seconds: u64,
}
impl Default for Scheduler {
    fn default() -> Self {
        Self {
            concurrency: 1,
            queue_capacity: 1024,
            poll_ms: 200,
            visible_lease_seconds: 6,
            prefetch_limit: 12,
            background_enabled: true,
            background_idle_seconds: 30,
            background_cooldown_seconds: 3,
            background_max_load_ratio: 0.35,
            background_min_memory_mb: 512,
            max_attempts: 3,
            retry_initial_seconds: 2,
            retry_max_seconds: 60,
            commit_max_attempts: 3,
            commit_retry_seconds: 1,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Fingerprint {
    #[default]
    Content,
    Metadata,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub fingerprint: Fingerprint,
    pub namespace: String,
    pub ttl_seconds: Option<u64>,
    pub cache_control: String,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            fingerprint: Fingerprint::Content,
            namespace: "gif-v1".into(),
            ttl_seconds: None,
            cache_control: "public, max-age=31536000, immutable".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Inputs {
    pub local_roots: Vec<PathBuf>,
    pub http_hosts: Vec<String>,
    pub allow_private_ips: bool,
    pub allow_http: bool,
}
impl Default for Inputs {
    fn default() -> Self {
        Self {
            local_roots: vec![".".into()],
            http_hosts: vec![],
            allow_private_ips: false,
            allow_http: false,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Output {
    Local {
        root: PathBuf,
    },
    S3 {
        store: String,
        prefix: String,
        signed_url_seconds: u64,
    },
}
impl Default for Output {
    fn default() -> Self {
        Self::Local {
            root: ".pipeline/artifacts".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub virtual_hosted_style: bool,
    pub allow_http: bool,
    pub access_key_env: String,
    pub secret_key_env: String,
    pub session_token_env: String,
}
impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: "us-east-1".into(),
            endpoint: None,
            virtual_hosted_style: false,
            allow_http: false,
            access_key_env: "AWS_ACCESS_KEY_ID".into(),
            secret_key_env: "AWS_SECRET_ACCESS_KEY".into(),
            session_token_env: "AWS_SESSION_TOKEN".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub listen: SocketAddr,
    pub token_env: Option<String>,
    pub max_body_bytes: usize,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8787".parse().expect("literal address"),
            token_env: None,
            max_body_bytes: 65536,
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        Self::load_with_env(path, std::env::vars().collect())
    }
    fn load_with_env(path: Option<&Path>, mut env: HashMap<String, String>) -> Result<Self> {
        // Apply typed defaults after merging explicit sources. Otherwise a switch
        // from local to S3 output leaves the local variant's root in the table.
        let mut b = config::Config::builder();
        if let Some(p) = path {
            b = b.add_source(config::File::from(p).required(true));
        }
        let mut list_overrides = vec![];
        for (name, key) in [
            ("PREVIEW__INPUTS__HTTP_HOSTS", "inputs.http_hosts"),
            ("PREVIEW__INPUTS__LOCAL_ROOTS", "inputs.local_roots"),
        ] {
            if let Some(value) = env.remove(name) {
                let values: Vec<_> = value
                    .split(',')
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned)
                    .collect();
                list_overrides.push((key, values));
            }
        }
        b = b.add_source(
            config::Environment::with_prefix("PREVIEW")
                .prefix_separator("__")
                .separator("__")
                .try_parsing(true)
                .source(Some(env)),
        );
        // Typed overrides also keep single numeric/boolean-looking paths as
        // strings; Environment's scalar inference otherwise runs before lists.
        for (key, values) in list_overrides {
            b = b
                .set_override(key, values)
                .map_err(|_| Error::Config("cannot set environment list".into()))?;
        }
        let c: Self = b.build().and_then(|c| c.try_deserialize()).map_err(|_| {
            Error::Config("cannot load configuration; check keys and value types".into())
        })?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        self.validate_recipe(&self.recipe)?;
        for recipe in self.recipes.values() {
            self.validate_recipe(recipe)?;
        }
        let e = &self.execution;
        let s = &self.scheduler;
        if e.threads == 0
            || e.threads > 256
            || e.timeout_seconds == 0
            || e.timeout_seconds > 604800
            || e.io_timeout_seconds == 0
            || e.io_timeout_seconds > 604800
            || e.max_input_bytes == 0
            || e.max_output_bytes == 0
            || e.format_whitelist.is_empty()
            || !e.max_duration_seconds.is_finite()
            || e.max_duration_seconds <= 0.0
            || !e.max_fps.is_finite()
            || e.max_fps <= 0.0
            || e.max_dimension < 2
        {
            return Err(Error::Config(
                "execution limits must be positive and finite".into(),
            ));
        }
        if s.concurrency == 0
            || s.concurrency > 256
            || s.queue_capacity == 0
            || s.queue_capacity > 1_000_000
            || s.max_attempts == 0
            || s.max_attempts > 100
            || s.poll_ms < 10
            || s.poll_ms > 60000
            || s.visible_lease_seconds == 0
            || s.visible_lease_seconds > 604800
            || s.background_idle_seconds > 604800
            || s.background_cooldown_seconds > 604800
            || s.prefetch_limit == 0
            || s.retry_initial_seconds == 0
            || s.retry_max_seconds < s.retry_initial_seconds
            || s.retry_max_seconds > 604800
            || s.commit_max_attempts == 0
            || s.commit_max_attempts > 100
            || s.commit_retry_seconds == 0
            || s.commit_retry_seconds > 604800
            || !s.background_max_load_ratio.is_finite()
            || s.background_max_load_ratio <= 0.0
        {
            return Err(Error::Config("invalid scheduler limits".into()));
        }
        if self.cache.namespace.is_empty()
            || self.cache.ttl_seconds == Some(0)
            || self.cache.cache_control.contains(['\r', '\n'])
            || self.server.max_body_bytes == 0
            || self.server.max_body_bytes > 16 * 1024 * 1024
        {
            return Err(Error::Config(
                "invalid cache or server configuration".into(),
            ));
        }
        for store in self.stores.values() {
            if store.bucket.is_empty() || store.region.is_empty() {
                return Err(Error::Config("S3 bucket and region are required".into()));
            }
        }
        if let Output::S3 {
            store,
            prefix,
            signed_url_seconds,
        } = &self.output
            && (!self.stores.contains_key(store)
                || *signed_url_seconds == 0
                || *signed_url_seconds > 604800
                || prefix.split('/').any(|v| v == ".."))
        {
            return Err(Error::Config("invalid S3 output configuration".into()));
        }
        Ok(())
    }
    pub fn validate_recipe(&self, recipe: &Recipe) -> Result<()> {
        recipe.validate()?;
        if recipe.duration_seconds > self.execution.max_duration_seconds
            || recipe.fps > self.execution.max_fps
            || [recipe.width, recipe.height]
                .into_iter()
                .flatten()
                .any(|v| v > self.execution.max_dimension)
        {
            return Err(Error::Invalid("recipe exceeds deployment limits".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_and_tagged_output_switch_are_valid() {
        Config::load(None).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("pipeline.toml");
        std::fs::write(&file, "[stores.media]\nbucket='bucket'\n[output]\nkind='s3'\nstore='media'\nprefix='gifs'\nsigned_url_seconds=60\n[recipe]\nfps=10.0\n").unwrap();
        let config = Config::load(Some(&file)).unwrap();
        assert!(matches!(config.output, Output::S3 { .. }));
        assert_eq!(config.recipe.fps, 10.0);
        assert_eq!(config.recipe.height, Some(240));
        assert_eq!(config.scheduler.concurrency, 1);
    }
    #[test]
    fn typos_and_zero_limits_fail() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("pipeline.toml");
        for contents in [
            "[recipe]\nfpps=10",
            "[scheduler]\nconcurrency=0",
            "[execution]\ntimeout_seconds=0",
            "[cache]\nttl_seconds=0",
            "[execution]\ntimeout_seconds=9223372036854775807",
            "[scheduler]\nbackground_cooldown_seconds=9223372036854775807",
            "[scheduler]\ncommit_max_attempts=0",
            "[scheduler]\ncommit_retry_seconds=0",
        ] {
            std::fs::write(&file, contents).unwrap();
            assert!(Config::load(Some(&file)).is_err(), "{contents}");
        }
    }
    #[test]
    fn environment_lists_override_toml_and_can_be_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("pipeline.toml");
        std::fs::write(
            &file,
            "[inputs]\nhttp_hosts=['old.example']\nlocal_roots=['old']\n",
        )
        .unwrap();
        let env = HashMap::from([
            (
                "PREVIEW__INPUTS__HTTP_HOSTS".into(),
                "one.example, two.example".into(),
            ),
            (
                "PREVIEW__INPUTS__LOCAL_ROOTS".into(),
                "/media/a, /media/b".into(),
            ),
            ("PREVIEW__SCHEDULER__CONCURRENCY".into(), "2".into()),
            ("PREVIEW__CACHE__NAMESPACE".into(), "a,b".into()),
        ]);
        let c = Config::load_with_env(Some(&file), env).unwrap();
        assert_eq!(c.inputs.http_hosts, ["one.example", "two.example"]);
        assert_eq!(
            c.inputs.local_roots,
            [PathBuf::from("/media/a"), PathBuf::from("/media/b")]
        );
        assert_eq!(c.scheduler.concurrency, 2);
        assert_eq!(c.cache.namespace, "a,b");
        let c = Config::load_with_env(
            Some(&file),
            HashMap::from([
                ("PREVIEW__INPUTS__HTTP_HOSTS".into(), String::new()),
                ("PREVIEW__INPUTS__LOCAL_ROOTS".into(), " , ".into()),
            ]),
        )
        .unwrap();
        assert!(c.inputs.http_hosts.is_empty());
        assert!(c.inputs.local_roots.is_empty());
        for literal in ["123", "true", "false"] {
            let c = Config::load_with_env(
                Some(&file),
                HashMap::from([("PREVIEW__INPUTS__LOCAL_ROOTS".into(), literal.into())]),
            )
            .unwrap();
            assert_eq!(c.inputs.local_roots, [PathBuf::from(literal)]);
        }
    }
}
