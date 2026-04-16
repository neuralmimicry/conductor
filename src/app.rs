use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Redirect},
    routing::{get, patch, post},
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    dashboard::render_dashboard,
    error::{ApiError, ApiResult},
    models::{NewWorkItem, WorkItem, WorkItemPatch},
    service::ConductorService,
};

#[derive(Debug, Default, Deserialize)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

pub fn build_router(service: ConductorService) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/healthz", get(health))
        .route("/dashboard", get(dashboard))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/services", get(services))
        .route("/api/v1/topology", get(topology))
        .route(
            "/api/v1/work-items",
            get(list_work_items).post(create_work_item),
        )
        .route("/api/v1/work-items/{id}", patch(update_work_item))
        .route("/api/v1/cycles", get(list_cycles))
        .route("/api/v1/discovery/runs", get(list_discovery_runs))
        .route("/api/v1/discovery/run", post(trigger_discovery))
        .route("/api/v1/planning/run", post(trigger_planning))
        .with_state(service)
}

async fn root() -> impl IntoResponse {
    Redirect::to("/dashboard")
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ok": true,
        "service": "conductor",
    }))
}

async fn dashboard(State(service): State<ConductorService>) -> impl IntoResponse {
    render_dashboard(&service.config.server.dashboard_title)
}

async fn summary(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<crate::models::DashboardSummary>> {
    service.authorize_read(&headers)?;
    Ok(Json(service.summary().await?))
}

async fn services(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(
        serde_json::json!({"services": service.services().await?}),
    ))
}

async fn topology(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<crate::models::TopologyGraph>> {
    service.authorize_read(&headers)?;
    Ok(Json(service.topology().await?))
}

async fn list_work_items(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(
        serde_json::json!({"work_items": service.work_items().await?}),
    ))
}

async fn create_work_item(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Json(request): Json<NewWorkItem>,
) -> ApiResult<Json<WorkItem>> {
    service.authorize_admin(&headers)?;
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title must not be empty"));
    }
    let item = WorkItem::from_new(request);
    service.repository.upsert_work_item(&item).await?;
    Ok(Json(item))
}

async fn update_work_item(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(patch): Json<WorkItemPatch>,
) -> ApiResult<Json<WorkItem>> {
    service.authorize_admin(&headers)?;
    let item = service
        .repository
        .patch_work_item(id, patch)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("work item {} not found", id)))?;
    Ok(Json(item))
}

async fn list_cycles(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let cycles = service
        .repository
        .list_improvement_cycles(query.limit.unwrap_or(20))
        .await?;
    Ok(Json(serde_json::json!({"cycles": cycles})))
}

async fn list_discovery_runs(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let runs = service
        .repository
        .list_discovery_runs(query.limit.unwrap_or(20))
        .await?;
    Ok(Json(serde_json::json!({"runs": runs})))
}

async fn trigger_discovery(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<crate::models::DiscoveryRun>> {
    service.authorize_admin(&headers)?;
    Ok(Json(service.run_discovery_cycle().await?))
}

async fn trigger_planning(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<crate::models::ImprovementCycle>> {
    service.authorize_admin(&headers)?;
    Ok(Json(service.run_planning_cycle().await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::util::ServiceExt;

    use crate::{
        config::ConductorConfig, integrations::build_http_client, service::ConductorService,
        storage::memory::MemoryRepository,
    };

    fn test_service() -> ConductorService {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        config.security.allow_dashboard_without_token = false;
        let repo = std::sync::Arc::new(MemoryRepository::new());
        let client = build_http_client(2).expect("client");
        ConductorService::new(config, repo, client)
    }

    #[tokio::test]
    async fn summary_requires_auth_when_token_is_configured() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn summary_is_readable_when_dashboard_reads_are_public() {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        config.security.allow_dashboard_without_token = true;
        let repo = std::sync::Arc::new(MemoryRepository::new());
        let client = build_http_client(2).expect("client");
        let app = build_router(ConductorService::new(config, repo, client));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn work_item_can_be_created_with_token() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/work-items")
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"title": "Probe", "summary": "Run a probe"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            payload.get("title").and_then(serde_json::Value::as_str),
            Some("Probe")
        );
    }
}
