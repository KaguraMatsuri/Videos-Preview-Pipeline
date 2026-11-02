//! Configurable, persistent video-to-GIF jobs shared by CLI and HTTP adapters.
pub mod config;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod model;
pub mod recipe;
pub mod runner;
pub mod scheduler;
pub mod state;
pub mod storage;
pub use config::Config;
pub use error::{Error, Result};
pub use model::{Input, Job, Priority, Status, Submit};
pub use scheduler::Pipeline;
