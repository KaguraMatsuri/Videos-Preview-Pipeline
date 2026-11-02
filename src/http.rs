use crate::{
    error::Error,
    model::{Priority, Status, Submit},
    scheduler::Pipeline,
    storage::ArtifactLocation,
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use serde::Deserialize;
use std::sync::Arc;

pub fn router(pipeline: Pipeline) -> crate::error::Result<Router> {
    let token = match &pipeline.config.server.token_env {
        Some(name) => Some(
            std::env::var(name)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| {
                    Error::Config("server token environment variable is missing or empty".into())
                })?,
        ),
        None => None,
    };
    if !pipeline.config.server.listen.ip().is_loopback() && token.is_none() {
        return Err(Error::Config(
            "non-loopback HTTP listeners require server.token_env".into(),
        ));
    }
    let limit = pipeline.config.server.max_body_bytes;
    Ok(Router::new()
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({"status":"ok"})) }),
        )
        .route("/jobs", post(submit).get(list))
        .route("/jobs/{id}", get(status))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/priority", post(prioritize))
        .route("/jobs/{id}/artifact", get(artifact))
        .route("/activity", post(activity))
        .layer(DefaultBodyLimit::max(limit))
        .layer(middleware::from_fn_with_state(Arc::new(token), authorize))
        .with_state(pipeline))
}
async fn authorize(
    State(token): State<Arc<Option<String>>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(token) = token.as_ref() {
        let expected = format!("Bearer {token}");
        if request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            != Some(expected.as_str())
        {
            return (StatusCode::UNAUTHORIZED,Json(serde_json::json!({"code":"unauthorized","message":"A valid bearer token is required"}))).into_response();
        }
    }
    next.run(request).await
}
async fn submit(
    State(p): State<Pipeline>,
    Json(request): Json<Submit>,
) -> Result<Response, ApiError> {
    Ok((StatusCode::ACCEPTED, Json(p.submit(request).await?)).into_response())
}
#[derive(Deserialize)]
struct Pagination {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}
fn default_limit() -> usize {
    50
}
async fn list(
    State(p): State<Pipeline>,
    Query(q): Query<Pagination>,
) -> Result<Response, ApiError> {
    Ok(Json(p.state.list(q.limit, q.offset).await?).into_response())
}
async fn status(State(p): State<Pipeline>, Path(id): Path<String>) -> Result<Response, ApiError> {
    Ok(Json(p.state.get(&id).await?).into_response())
}
async fn cancel(State(p): State<Pipeline>, Path(id): Path<String>) -> Result<Response, ApiError> {
    Ok(Json(p.cancel(&id).await?).into_response())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriorityRequest {
    priority: Priority,
}
async fn prioritize(
    State(p): State<Pipeline>,
    Path(id): Path<String>,
    Json(r): Json<PriorityRequest>,
) -> Result<Response, ApiError> {
    Ok(Json(p.prioritize(&id, r.priority).await?).into_response())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Activity {
    #[serde(default)]
    background_paused: bool,
}
async fn activity(
    State(p): State<Pipeline>,
    Json(r): Json<Activity>,
) -> Result<Response, ApiError> {
    p.mark_interactive().await?;
    p.set_background_paused(r.background_paused);
    Ok(StatusCode::NO_CONTENT.into_response())
}
async fn artifact(State(p): State<Pipeline>, Path(id): Path<String>) -> Result<Response, ApiError> {
    let job = p.state.get(&id).await?;
    if !job.status.terminal() {
        return Ok((
            StatusCode::ACCEPTED,
            [(header::RETRY_AFTER, "2")],
            Json(job),
        )
            .into_response());
    }
    if job.status != Status::Ready {
        return Ok((StatusCode::CONFLICT, Json(job)).into_response());
    }
    let artifact = job.artifact.ok_or(Error::NotFound)?;
    match p.storage.location(&artifact.key).await? {
        ArtifactLocation::Redirect(url) => Ok((
            [(header::CACHE_CONTROL, "private, no-store")],
            Redirect::temporary(&url),
        )
            .into_response()),
        ArtifactLocation::Local(path) => {
            let file = tokio::fs::File::open(path).await.map_err(Error::Io)?;
            let body = Body::from_stream(tokio_util::io::ReaderStream::new(file));
            Ok((
                [
                    (header::CONTENT_TYPE, "image/gif"),
                    (header::CACHE_CONTROL, p.config.cache.cache_control.as_str()),
                ],
                body,
            )
                .into_response())
        }
    }
}
struct ApiError(Error);
impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            Error::Invalid(_) | Error::Json(_) => StatusCode::BAD_REQUEST,
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::QueueFull => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({"code":self.0.code(),"message":self.0.to_string()})),
        )
            .into_response()
    }
}
