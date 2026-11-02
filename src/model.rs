use crate::recipe::{Recipe, RecipeOverrides};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    Local { path: PathBuf },
    Http { url: String },
    S3 { store: String, key: String },
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Visible,
    #[default]
    Prefetch,
    Background,
}
impl Priority {
    pub fn rank(self) -> i32 {
        match self {
            Self::Visible => 0,
            Self::Prefetch => 1,
            Self::Background => 2,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submit {
    pub input: Input,
    #[serde(default)]
    pub recipe: Option<String>,
    #[serde(default)]
    pub overrides: RecipeOverrides,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub force: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub input: Input,
    pub recipe: Recipe,
    pub force: bool,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Generating,
    Ready,
    Error,
    Cancelled,
}
impl Status {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Ready | Self::Error | Self::Cancelled)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Generating => "generating",
            Self::Ready => "ready",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub key: String,
    pub size: u64,
    pub cache_hit: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub status: Status,
    pub priority: Priority,
    pub attempts: u32,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub recipe: Recipe,
    pub artifact: Option<Artifact>,
    pub error: Option<String>,
}
#[derive(Clone, Debug)]
pub(crate) struct Claimed {
    pub job: Job,
    pub task: Task,
}
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
