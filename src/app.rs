use std::{convert::Infallible, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{
        IntoResponse, Redirect,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::stream;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    dashboard::render_dashboard,
    error::{ApiError, ApiResult},
    models::{NewWorkItem, WorkExecution, WorkItem, WorkItemPatch},
    service::ConductorService,
};

#[derive(Debug, Default, Deserialize)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ExecuteWorkItemRequest {
    pub force_schedule: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct EventStreamQuery {
    pub token: Option<String>,
}

pub fn build_router(service: ConductorService) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/healthz", get(health))
        .route("/dashboard", get(dashboard))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/findings", get(findings))
        .route("/api/v1/findings/{id}", get(get_finding))
        .route("/api/v1/findings/{id}/evidence", get(get_finding_evidence))
        .route(
            "/api/v1/findings/{id}/provenance",
            get(get_finding_provenance),
        )
        .route("/api/v1/repositories", get(repositories))
        .route("/api/v1/services", get(services))
        .route("/api/v1/topology", get(topology))
        .route("/api/v1/events", get(list_events))
        .route("/api/v1/executions", get(list_executions))
        .route("/api/v1/executions/stream", get(stream_executions))
        .route("/api/v1/execution/run", post(trigger_execution_cycle))
        .route(
            "/api/v1/work-items",
            get(list_work_items).post(create_work_item),
        )
        .route(
            "/api/v1/work-items/{id}",
            get(get_work_item).patch(update_work_item),
        )
        .route(
            "/api/v1/work-items/{id}/executions",
            get(list_work_item_executions),
        )
        .route("/api/v1/work-items/{id}/execute", post(execute_work_item))
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

async fn repositories(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(
        serde_json::json!({"repositories": service.repositories().await?}),
    ))
}

async fn findings(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(
        serde_json::json!({"findings": service.findings().await?}),
    ))
}

async fn get_finding(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let finding = service
        .finding(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("finding {} not found", id)))?;
    Ok(Json(serde_json::json!({"finding": finding})))
}

async fn get_finding_evidence(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    service
        .finding(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("finding {} not found", id)))?;
    Ok(Json(serde_json::json!({
        "evidence": service.finding_evidence(id).await?
    })))
}

async fn get_finding_provenance(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    service
        .finding(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("finding {} not found", id)))?;
    Ok(Json(serde_json::json!({
        "provenance": service.finding_provenance(id).await?
    })))
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

async fn get_work_item(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<WorkItem>> {
    service.authorize_read(&headers)?;
    let item = service
        .work_item(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("work item {} not found", id)))?;
    Ok(Json(item))
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

async fn list_executions(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let executions = service.executions(query.limit.unwrap_or(50)).await?;
    Ok(Json(serde_json::json!({"executions": executions})))
}

async fn list_events(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let events = service.events(query.limit.unwrap_or(100)).await?;
    Ok(Json(serde_json::json!({"events": events})))
}

async fn stream_executions(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Query(query): Query<EventStreamQuery>,
) -> ApiResult<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>> {
    service.authorize_read_with_token(&headers, query.token.as_deref())?;
    let receiver = service.subscribe_events();
    let stream = stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(event) => {
                let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
                Some((
                    Ok::<Event, Infallible>(Event::default().data(payload)),
                    receiver,
                ))
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                let payload = json!({
                    "event_type": "execution.stream.lagged",
                    "message": format!("execution stream skipped {} event(s)", skipped),
                    "skipped": skipped,
                });
                Some((
                    Ok::<Event, Infallible>(Event::default().data(payload.to_string())),
                    receiver,
                ))
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
        }
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn list_work_item_executions(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(query): Query<LimitQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    service
        .work_item(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("work item {} not found", id)))?;
    let executions = service
        .work_item_executions(id, query.limit.unwrap_or(20))
        .await?;
    Ok(Json(serde_json::json!({"executions": executions})))
}

async fn trigger_execution_cycle(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_admin(&headers)?;
    let executions = service.run_execution_cycle().await?;
    Ok(Json(serde_json::json!({"executions": executions})))
}

async fn execute_work_item(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<ExecuteWorkItemRequest>,
) -> ApiResult<Json<WorkExecution>> {
    service.authorize_admin(&headers)?;
    let execution = service
        .execute_work_item(id, request.force_schedule.unwrap_or(false))
        .await
        .map_err(|error| {
            let message = error.to_string();
            if message.contains("not found") {
                ApiError::not_found(message)
            } else if message.contains("not approved") {
                ApiError::bad_request(message)
            } else {
                ApiError::internal(message)
            }
        })?;
    Ok(Json(execution))
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

    use crate::models::{
        FindingRecord, FindingSeverity, FindingStatus, NewWorkItem, WorkExecution, WorkItem,
        now_utc,
    };
    use crate::{
        config::ConductorConfig, integrations::build_http_client, service::ConductorService,
        storage::memory::MemoryRepository,
    };
    use serde_json::json;

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
    async fn execution_stream_requires_auth_when_token_is_configured() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/executions/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn execution_stream_accepts_query_token_and_returns_sse() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/executions/stream?token=secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/event-stream"))
        );
    }

    #[tokio::test]
    async fn repositories_require_auth_when_token_is_configured() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/repositories")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn findings_require_auth_when_token_is_configured() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/findings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn findings_endpoint_returns_records_when_authorized() {
        let service = test_service();
        let finding = FindingRecord {
            id: uuid::Uuid::new_v4(),
            finding_key: "repository_test_baseline:gail".to_string(),
            title: "Gail lacks tests".to_string(),
            summary: "Test baseline missing".to_string(),
            category: "testability".to_string(),
            severity: FindingSeverity::Medium,
            status: FindingStatus::Open,
            target_service: Some("gail".to_string()),
            target_repository: Some("gail".to_string()),
            source_run_id: None,
            confidence_score: 0.8,
            tags: vec!["tests".to_string()],
            details: json!({"rule": "repository_missing_tests_capability"}),
            first_seen_at: now_utc(),
            last_seen_at: now_utc(),
            updated_at: now_utc(),
        };
        service
            .repository
            .replace_findings(&[finding.clone()], &[], &[])
            .await
            .expect("replace findings");

        let app = build_router(service);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/findings")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
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
            payload["findings"][0]["finding_key"].as_str(),
            Some("repository_test_baseline:gail")
        );
    }

    #[tokio::test]
    async fn service_broadcasts_published_events() {
        let service = test_service();
        let mut receiver = service.subscribe_events();
        service.publish_event(crate::models::ConductorEvent::new(
            "execution.test",
            "hello",
            json!({"ok": true}),
        ));

        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timeout")
            .expect("event");
        assert_eq!(event.event_type, "execution.test");
        assert_eq!(event.message, "hello");

        let persisted = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let events = service.events(10).await.expect("events");
                if !events.is_empty() {
                    break events;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("persisted events");
        assert_eq!(persisted[0].event_type, "execution.test");
    }

    #[tokio::test]
    async fn events_endpoint_requires_auth_when_token_is_configured() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

    #[tokio::test]
    async fn work_item_execution_history_endpoint_returns_records() {
        let service = test_service();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("probe:history".to_string()),
            title: "Probe".to_string(),
            summary: "Record execution history".to_string(),
            target_service: Some("refiner".to_string()),
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec!["automation".to_string()],
            plan: json!({"action": "probe"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        service
            .repository
            .upsert_work_item(&item)
            .await
            .expect("work item");
        let execution = WorkExecution::new(item.id, item.target_service.clone());
        service
            .repository
            .upsert_work_execution(&execution)
            .await
            .expect("execution");

        let app = build_router(service);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/work-items/{}/executions", item.id))
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
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
            payload["executions"]
                .as_array()
                .expect("execution list")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn execute_work_item_route_returns_not_found_for_missing_item() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/work-items/{}/execute", Uuid::new_v4()))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
