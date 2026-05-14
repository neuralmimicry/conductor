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
use tower_http::services::ServeDir;
use uuid::Uuid;

use crate::{
    dashboard::render_dashboard,
    error::{ApiError, ApiResult},
    models::{
        ConfluencePageLinkRequest, JiraIssueLinkRequest, NewTraceabilityLink, NewWorkItem,
        TraceabilitySyncRequest, WorkExecution, WorkItem, WorkItemPatch,
    },
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
        .nest_service("/assets", ServeDir::new("assets"))
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
        .route("/api/v1/traceability/graph", get(get_traceability_graph))
        .route("/api/v1/events", get(list_events))
        .route("/api/v1/executions", get(list_executions))
        .route("/api/v1/executions/stream", get(stream_executions))
        .route("/api/v1/execution/run", post(trigger_execution_cycle))
        .route("/api/v1/links/sync", post(sync_all_links))
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
        .route(
            "/api/v1/work-items/{id}/links",
            get(list_work_item_links).post(create_work_item_link),
        )
        .route(
            "/api/v1/work-items/{id}/links/sync",
            post(sync_work_item_links),
        )
        .route(
            "/api/v1/work-items/{id}/links/jira",
            post(link_work_item_jira),
        )
        .route(
            "/api/v1/work-items/{id}/links/confluence",
            post(link_work_item_confluence),
        )
        .route(
            "/api/v1/work-items/{id}/traceability",
            get(get_work_item_traceability),
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
    render_dashboard(
        &service.config.server.dashboard_title,
        env!("CARGO_PKG_VERSION"),
    )
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

async fn get_traceability_graph(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(serde_json::json!({
        "graph": service.traceability_graph().await?
    })))
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

async fn list_work_item_links(
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
    let links = service
        .work_item_links(id, query.limit.unwrap_or(100))
        .await?;
    Ok(Json(serde_json::json!({"links": links})))
}

async fn create_work_item_link(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<NewTraceabilityLink>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_admin(&headers)?;
    if request.system.trim().is_empty() {
        return Err(ApiError::bad_request("system must not be empty"));
    }
    if request.reference_type.trim().is_empty() {
        return Err(ApiError::bad_request("reference_type must not be empty"));
    }
    if request.reference_key.trim().is_empty() {
        return Err(ApiError::bad_request("reference_key must not be empty"));
    }
    let link = service
        .upsert_work_item_link(id, request)
        .await
        .map_err(|error| {
            let message = error.to_string();
            if message.contains("not found") {
                ApiError::not_found(message)
            } else {
                ApiError::internal(message)
            }
        })?;
    Ok(Json(serde_json::json!({"link": link})))
}

async fn link_work_item_jira(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<JiraIssueLinkRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_admin(&headers)?;
    if !request.fields.is_null() && !request.fields.is_object() {
        return Err(ApiError::bad_request("fields must be a JSON object"));
    }
    let result = service
        .link_work_item_jira(id, request)
        .await
        .map_err(map_service_error)?;
    Ok(Json(serde_json::json!({"result": result})))
}

async fn link_work_item_confluence(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<ConfluencePageLinkRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_admin(&headers)?;
    let result = service
        .link_work_item_confluence(id, request)
        .await
        .map_err(map_service_error)?;
    Ok(Json(serde_json::json!({"result": result})))
}

async fn sync_work_item_links(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<TraceabilitySyncRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_admin(&headers)?;
    let result = service
        .sync_work_item_links(id, request)
        .await
        .map_err(map_service_error)?;
    Ok(Json(serde_json::json!({"sync": result})))
}

async fn sync_all_links(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Json(request): Json<TraceabilitySyncRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_admin(&headers)?;
    let result = service
        .sync_all_links(request)
        .await
        .map_err(map_service_error)?;
    Ok(Json(serde_json::json!({"sync": result})))
}

async fn get_work_item_traceability(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let traceability = service
        .work_item_traceability(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("work item {} not found", id)))?;
    Ok(Json(serde_json::json!({"traceability": traceability})))
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

fn map_service_error(error: anyhow::Error) -> ApiError {
    let message = error.to_string();
    if message.contains("not found") {
        ApiError::not_found(message)
    } else if message.contains("must not")
        || message.contains("configured")
        || message.contains("disabled")
        || message.contains("transition")
    {
        ApiError::bad_request(message)
    } else {
        ApiError::internal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::State as AxumState,
        http::{Request, StatusCode},
        routing::{get, post, put},
    };
    use std::{collections::HashMap, sync::Arc};
    use tokio::{net::TcpListener, sync::Mutex};
    use tower::util::ServiceExt;

    use crate::models::{
        FindingEvidence, FindingProvenance, FindingRecord, FindingSeverity, FindingStatus,
        NewTraceabilityLink, NewWorkItem, RepositorySnapshot, ServiceHealth, ServiceSnapshot,
        WorkExecution, WorkItem, now_utc,
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

    fn test_service_with_atlassian(base_url: String) -> ConductorService {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        config.security.allow_dashboard_without_token = false;
        config.integrations.atlassian.enabled = true;
        config.integrations.atlassian.base_url = Some(base_url);
        config.integrations.atlassian.username = Some("user@example.com".to_string());
        config.integrations.atlassian.api_token = Some("token".to_string());
        config.integrations.atlassian.jira_project_key = Some("KAN".to_string());
        config.integrations.atlassian.confluence_space_key = Some("ENG".to_string());
        config.integrations.atlassian.sync_interval_seconds = 0;
        let repo = std::sync::Arc::new(MemoryRepository::new());
        let client = build_http_client(2).expect("client");
        ConductorService::new(config, repo, client)
    }

    #[derive(Clone, Default)]
    struct MockAtlassianState {
        issue: Arc<Mutex<Option<MockIssue>>>,
        page: Arc<Mutex<Option<MockPage>>>,
    }

    #[derive(Clone)]
    struct MockIssue {
        id: String,
        key: String,
        project_key: String,
        summary: String,
        issue_type: String,
        status: String,
        labels: Vec<String>,
    }

    #[derive(Clone)]
    struct MockPage {
        id: String,
        title: String,
        space_key: String,
        version_number: i64,
        labels: Vec<String>,
    }

    async fn mock_jira_search(
        AxumState(state): AxumState<MockAtlassianState>,
        Query(_query): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        let issue = state.issue.lock().await.clone();
        Json(json!({
            "issues": issue.into_iter().map(|issue| issue_json(&issue)).collect::<Vec<_>>()
        }))
    }

    async fn mock_jira_create(
        AxumState(state): AxumState<MockAtlassianState>,
        Json(payload): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let fields = payload
            .get("fields")
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        let issue = MockIssue {
            id: "10001".to_string(),
            key: "KAN-99".to_string(),
            project_key: fields
                .get("project")
                .and_then(|value| value.get("key"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("KAN")
                .to_string(),
            summary: fields
                .get("summary")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("generated issue")
                .to_string(),
            issue_type: fields
                .get("issuetype")
                .and_then(|value| value.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Task")
                .to_string(),
            status: "To Do".to_string(),
            labels: fields
                .get("labels")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToString::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        };
        *state.issue.lock().await = Some(issue.clone());
        Json(json!({"id": issue.id, "key": issue.key}))
    }

    async fn mock_jira_get(
        AxumState(state): AxumState<MockAtlassianState>,
        Path(issue_key): Path<String>,
    ) -> axum::response::Response {
        let issue = state.issue.lock().await.clone();
        match issue.filter(|issue| issue.key == issue_key) {
            Some(issue) => (StatusCode::OK, Json(issue_json(&issue))).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "issue not found"})),
            )
                .into_response(),
        }
    }

    async fn mock_jira_update(
        AxumState(state): AxumState<MockAtlassianState>,
        Path(issue_key): Path<String>,
        Json(payload): Json<serde_json::Value>,
    ) -> axum::response::Response {
        let mut guard = state.issue.lock().await;
        let Some(issue) = guard.as_mut().filter(|issue| issue.key == issue_key) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "issue not found"})),
            )
                .into_response();
        };
        let fields = payload
            .get("fields")
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(summary) = fields.get("summary").and_then(serde_json::Value::as_str) {
            issue.summary = summary.to_string();
        }
        if let Some(labels) = fields.get("labels").and_then(serde_json::Value::as_array) {
            issue.labels = labels
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToString::to_string)
                .collect();
        }
        (StatusCode::OK, Json(json!({}))).into_response()
    }

    async fn mock_jira_transitions() -> Json<serde_json::Value> {
        Json(json!({"transitions": [{"id": "31", "name": "Done"}]}))
    }

    async fn mock_jira_transition(
        AxumState(state): AxumState<MockAtlassianState>,
        Path(issue_key): Path<String>,
        Json(payload): Json<serde_json::Value>,
    ) -> axum::response::Response {
        let mut guard = state.issue.lock().await;
        let Some(issue) = guard.as_mut().filter(|issue| issue.key == issue_key) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "issue not found"})),
            )
                .into_response();
        };
        if payload
            .get("transition")
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
            == Some("31")
        {
            issue.status = "Done".to_string();
        }
        (StatusCode::OK, Json(json!({}))).into_response()
    }

    async fn mock_confluence_search(
        AxumState(state): AxumState<MockAtlassianState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        let title = query.get("title").cloned().unwrap_or_default();
        let space_key = query.get("spaceKey").cloned().unwrap_or_default();
        let page = state.page.lock().await.clone().filter(|page| {
            page.title == title && (space_key.is_empty() || page.space_key == space_key)
        });
        Json(json!({
            "results": page.into_iter().map(|page| page_json(&page)).collect::<Vec<_>>()
        }))
    }

    async fn mock_confluence_create(
        AxumState(state): AxumState<MockAtlassianState>,
        Json(payload): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let page = MockPage {
            id: "851969".to_string(),
            title: payload
                .get("title")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("generated page")
                .to_string(),
            space_key: payload
                .get("space")
                .and_then(|value| value.get("key"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("ENG")
                .to_string(),
            version_number: 1,
            labels: Vec::new(),
        };
        *state.page.lock().await = Some(page.clone());
        Json(json!({"id": page.id, "title": page.title}))
    }

    async fn mock_confluence_get(
        AxumState(state): AxumState<MockAtlassianState>,
        Path(page_id): Path<String>,
    ) -> axum::response::Response {
        let page = state.page.lock().await.clone();
        match page.filter(|page| page.id == page_id) {
            Some(page) => (StatusCode::OK, Json(page_json(&page))).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "page not found"})),
            )
                .into_response(),
        }
    }

    async fn mock_confluence_update(
        AxumState(state): AxumState<MockAtlassianState>,
        Path(page_id): Path<String>,
        Json(payload): Json<serde_json::Value>,
    ) -> axum::response::Response {
        let mut guard = state.page.lock().await;
        let Some(page) = guard.as_mut().filter(|page| page.id == page_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "page not found"})),
            )
                .into_response();
        };
        if let Some(title) = payload.get("title").and_then(serde_json::Value::as_str) {
            page.title = title.to_string();
        }
        page.version_number += 1;
        (StatusCode::OK, Json(json!({}))).into_response()
    }

    async fn mock_confluence_labels(
        AxumState(state): AxumState<MockAtlassianState>,
        Path(page_id): Path<String>,
        Json(payload): Json<serde_json::Value>,
    ) -> axum::response::Response {
        let mut guard = state.page.lock().await;
        let Some(page) = guard.as_mut().filter(|page| page.id == page_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "page not found"})),
            )
                .into_response();
        };
        page.labels = payload
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.get("name").and_then(serde_json::Value::as_str))
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();
        (StatusCode::OK, Json(json!({}))).into_response()
    }

    fn issue_json(issue: &MockIssue) -> serde_json::Value {
        json!({
            "id": issue.id,
            "key": issue.key,
            "fields": {
                "summary": issue.summary,
                "issuetype": {"name": issue.issue_type},
                "status": {"name": issue.status},
                "labels": issue.labels,
                "updated": "2026-04-23T10:00:00+00:00",
                "project": {"key": issue.project_key},
            }
        })
    }

    fn page_json(page: &MockPage) -> serde_json::Value {
        json!({
            "id": page.id,
            "title": page.title,
            "space": {"key": page.space_key},
            "version": {"number": page.version_number, "when": "2026-04-23T10:00:00+00:00"},
            "metadata": {
                "labels": {
                    "results": page.labels.iter().map(|label| json!({"name": label})).collect::<Vec<_>>()
                }
            }
        })
    }

    async fn spawn_mock_atlassian() -> (String, MockAtlassianState, tokio::task::JoinHandle<()>) {
        let state = MockAtlassianState::default();
        let app = Router::new()
            .route("/rest/api/3/search/jql", get(mock_jira_search))
            .route("/rest/api/2/issue", post(mock_jira_create))
            .route("/rest/api/3/issue/{issue_key}", get(mock_jira_get))
            .route("/rest/api/2/issue/{issue_key}", put(mock_jira_update))
            .route(
                "/rest/api/2/issue/{issue_key}/transitions",
                get(mock_jira_transitions).post(mock_jira_transition),
            )
            .route(
                "/wiki/rest/api/content",
                get(mock_confluence_search).post(mock_confluence_create),
            )
            .route(
                "/wiki/rest/api/content/{page_id}",
                get(mock_confluence_get).put(mock_confluence_update),
            )
            .route(
                "/wiki/rest/api/content/{page_id}/label",
                post(mock_confluence_labels),
            )
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let base_url = format!("http://{}", listener.local_addr().expect("addr"));
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });
        (base_url, state, handle)
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
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
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
        let execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
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
    async fn work_item_link_endpoints_round_trip() {
        let service = test_service();
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("link:history".to_string()),
            title: "Link Atlassian references".to_string(),
            summary: "Persist external references for a work item.".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"finding_key": "repository_test_baseline:conductor"}),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        service
            .repository
            .upsert_work_item(&item)
            .await
            .expect("work item");

        let app = build_router(service);
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/work-items/{}/links", item.id))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "system": "jira",
                            "reference_type": "bug",
                            "reference_key": "KAN-5",
                            "status": "To Do",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create response");
        assert_eq!(create.status(), StatusCode::OK);
        let create_body = to_bytes(create.into_body(), usize::MAX)
            .await
            .expect("create body");
        let create_payload: serde_json::Value =
            serde_json::from_slice(&create_body).expect("create json");
        assert_eq!(
            create_payload["link"]["finding_key"].as_str(),
            Some("repository_test_baseline:conductor")
        );

        let list = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/work-items/{}/links", item.id))
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("list response");
        assert_eq!(list.status(), StatusCode::OK);
        let list_body = to_bytes(list.into_body(), usize::MAX)
            .await
            .expect("list body");
        let list_payload: serde_json::Value =
            serde_json::from_slice(&list_body).expect("list json");
        assert_eq!(list_payload["links"].as_array().expect("links").len(), 1);
        assert_eq!(
            list_payload["links"][0]["reference_key"].as_str(),
            Some("KAN-5")
        );
    }

    #[tokio::test]
    async fn work_item_traceability_endpoint_returns_finding_and_validation() {
        let service = test_service();
        let finding = FindingRecord {
            id: uuid::Uuid::new_v4(),
            finding_key: "repository_test_baseline:conductor".to_string(),
            title: "Conductor lacks verification coverage".to_string(),
            summary: "Independent validation needs to be visible in traceability views."
                .to_string(),
            category: "validation".to_string(),
            severity: FindingSeverity::High,
            status: FindingStatus::Open,
            target_service: Some("conductor".to_string()),
            target_repository: Some("conductor".to_string()),
            source_run_id: None,
            confidence_score: 0.92,
            tags: vec!["validation".to_string()],
            details: json!({"gap": "traceability"}),
            first_seen_at: now_utc(),
            last_seen_at: now_utc(),
            updated_at: now_utc(),
        };
        let evidence = FindingEvidence {
            id: uuid::Uuid::new_v4(),
            finding_id: finding.id,
            evidence_type: "repository_snapshot".to_string(),
            source_kind: "inventory".to_string(),
            source_ref: "conductor".to_string(),
            summary: "Conductor repository inventory".to_string(),
            payload: json!({"repo_key": "conductor"}),
            collected_at: now_utc(),
        };
        let provenance = FindingProvenance {
            id: uuid::Uuid::new_v4(),
            finding_id: finding.id,
            stage: "planning".to_string(),
            origin: "deterministic".to_string(),
            component: "conductor.findings".to_string(),
            detail: "Finding carried into traceability view".to_string(),
            confidence_score: Some(0.92),
            payload: json!({"source": "test"}),
            recorded_at: now_utc(),
        };
        service
            .repository
            .replace_findings(&[finding.clone()], &[evidence], &[provenance])
            .await
            .expect("findings");

        service
            .repository
            .replace_service_snapshots(&[ServiceSnapshot {
                service_key: "conductor".to_string(),
                display_name: "Conductor".to_string(),
                kind: "tenant_service".to_string(),
                role_name: "continuum_tenant_conductor".to_string(),
                playbooks: vec![],
                host_targets: vec![],
                hosts: vec![],
                namespace: None,
                service_name: None,
                deployment_environment: None,
                internal_url: None,
                public_url: None,
                repo_path: Some("/tmp/conductor".to_string()),
                repo_url: Some("git@github.com:neuralmimicry/conductor.git".to_string()),
                repo_branch: Some("main".to_string()),
                health: ServiceHealth::Healthy,
                capabilities: vec!["orchestration".to_string()],
                dependencies: vec![],
                storage_paths: vec![],
                raw_defaults: json!({}),
                probe: json!({}),
                discovered_at: now_utc(),
                updated_at: now_utc(),
            }])
            .await
            .expect("services");
        service
            .repository
            .replace_repository_snapshots(&[RepositorySnapshot {
                repo_key: "conductor".to_string(),
                name: "conductor".to_string(),
                owner: Some("neuralmimicry".to_string()),
                repo_url: Some("git@github.com:neuralmimicry/conductor.git".to_string()),
                local_path: Some("/tmp/conductor".to_string()),
                default_branch: Some("main".to_string()),
                current_branch: Some("main".to_string()),
                language: Some("Rust".to_string()),
                frameworks: vec!["axum".to_string()],
                build_systems: vec!["cargo".to_string()],
                package_managers: vec!["cargo".to_string()],
                runtime_type: Some("service".to_string()),
                deployment_type: Some("container".to_string()),
                purpose: Some("control_plane".to_string()),
                criticality: "high".to_string(),
                visibility: Some("private".to_string()),
                archived: false,
                linked_services: vec!["conductor".to_string()],
                dependencies: vec![],
                capabilities: vec!["orchestration".to_string()],
                inventory_sources: vec!["local".to_string()],
                metadata: json!({}),
                discovered_at: now_utc(),
                updated_at: now_utc(),
            }])
            .await
            .expect("repositories");

        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("traceability:conductor".to_string()),
            title: "Expose validation traceability".to_string(),
            summary: "Surface evidence, execution, and validation from one endpoint.".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: None,
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec!["traceability".to_string()],
            plan: json!({
                "action": "expose_traceability",
                "finding_id": finding.id.to_string(),
                "finding_key": finding.finding_key.clone(),
            }),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        service
            .repository
            .upsert_work_item(&item)
            .await
            .expect("work item");

        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
        execution.verification = json!({
            "passed": true,
            "independent_validation": {
                "passed": true,
                "completeness": "full",
                "summary": "independent validation passed"
            }
        });
        service
            .repository
            .upsert_work_execution(&execution)
            .await
            .expect("execution");
        service
            .upsert_work_item_link(
                item.id,
                NewTraceabilityLink {
                    execution_id: Some(execution.id),
                    finding_key: Some(finding.finding_key.clone()),
                    system: "jira".to_string(),
                    reference_type: "bug".to_string(),
                    reference_key: "KAN-5".to_string(),
                    title: Some("Independent validation follow-up".to_string()),
                    status: Some("To Do".to_string()),
                    url: None,
                    metadata: json!({"issue_type": "bug"}),
                },
            )
            .await
            .expect("link");

        let app = build_router(service);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/work-items/{}/traceability", item.id))
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
        let finding_id = finding.id.to_string();
        assert_eq!(
            payload["traceability"]["finding"]["id"].as_str(),
            Some(finding_id.as_str())
        );
        assert_eq!(
            payload["traceability"]["target_repository"]["repo_key"].as_str(),
            Some("conductor")
        );
        assert_eq!(
            payload["traceability"]["independent_validation"]["completeness"].as_str(),
            Some("full")
        );
        assert_eq!(
            payload["traceability"]["links"]
                .as_array()
                .expect("links")
                .len(),
            1
        );
        assert_eq!(
            payload["traceability"]["executions"]
                .as_array()
                .expect("executions")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn traceability_graph_endpoint_returns_estate_graph() {
        let service = test_service();
        let finding = FindingRecord {
            id: uuid::Uuid::new_v4(),
            finding_key: "rollout:tracey".to_string(),
            title: "Tracey rollout evidence is fragmented".to_string(),
            summary: "Estate graph should show the change path from finding to rollout telemetry."
                .to_string(),
            category: "traceability".to_string(),
            severity: FindingSeverity::Medium,
            status: FindingStatus::Open,
            target_service: Some("tracey".to_string()),
            target_repository: Some("tracey".to_string()),
            source_run_id: None,
            confidence_score: 0.87,
            tags: vec!["traceability".to_string(), "rollout".to_string()],
            details: json!({"gap": "estate_graph"}),
            first_seen_at: now_utc(),
            last_seen_at: now_utc(),
            updated_at: now_utc(),
        };
        service
            .repository
            .replace_findings(&[finding.clone()], &[], &[])
            .await
            .expect("findings");
        service
            .repository
            .replace_service_snapshots(&[ServiceSnapshot {
                service_key: "tracey".to_string(),
                display_name: "Tracey".to_string(),
                kind: "tenant_service".to_string(),
                role_name: "continuum_tenant_tracey".to_string(),
                playbooks: vec!["tracey.yml".to_string()],
                host_targets: vec!["gpu".to_string()],
                hosts: vec!["tracey-1".to_string()],
                namespace: Some("tracey".to_string()),
                service_name: Some("tracey".to_string()),
                deployment_environment: Some(crate::models::DeliveryStage::Production),
                internal_url: Some("http://tracey.tracey.svc.cluster.local:48000".to_string()),
                public_url: Some("https://tracey.neuralmimicry.ai".to_string()),
                repo_path: Some("/tmp/tracey".to_string()),
                repo_url: Some("git@github.com:neuralmimicry/tracey.git".to_string()),
                repo_branch: Some("main".to_string()),
                health: ServiceHealth::Degraded,
                capabilities: vec!["runtime_analysis".to_string()],
                dependencies: vec!["refiner".to_string()],
                storage_paths: vec!["/var/lib/tracey".to_string()],
                raw_defaults: json!({}),
                probe: json!({}),
                discovered_at: now_utc(),
                updated_at: now_utc(),
            }])
            .await
            .expect("services");
        service
            .repository
            .replace_repository_snapshots(&[RepositorySnapshot {
                repo_key: "tracey".to_string(),
                name: "tracey".to_string(),
                owner: Some("neuralmimicry".to_string()),
                repo_url: Some("git@github.com:neuralmimicry/tracey.git".to_string()),
                local_path: Some("/tmp/tracey".to_string()),
                default_branch: Some("main".to_string()),
                current_branch: Some("main".to_string()),
                language: Some("Rust".to_string()),
                frameworks: vec!["axum".to_string()],
                build_systems: vec!["cargo".to_string()],
                package_managers: vec!["cargo".to_string()],
                runtime_type: Some("service".to_string()),
                deployment_type: Some("container".to_string()),
                purpose: Some("runtime_analysis".to_string()),
                criticality: "high".to_string(),
                visibility: Some("private".to_string()),
                archived: false,
                linked_services: vec!["tracey".to_string()],
                dependencies: vec!["rag_demo".to_string()],
                capabilities: vec!["runtime_analysis".to_string()],
                inventory_sources: vec!["local".to_string()],
                metadata: json!({}),
                discovered_at: now_utc(),
                updated_at: now_utc(),
            }])
            .await
            .expect("repositories");

        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("traceability:graph".to_string()),
            title: "Expose estate traceability graph".to_string(),
            summary: "Link findings, rollout evidence, and runtime state across the estate."
                .to_string(),
            target_service: Some("tracey".to_string()),
            delivery_stage: Some(crate::models::DeliveryStage::Production),
            validated_stages: vec![
                crate::models::DeliveryStage::Development,
                crate::models::DeliveryStage::Testing,
                crate::models::DeliveryStage::Integration,
                crate::models::DeliveryStage::IntegrationTesting,
                crate::models::DeliveryStage::Uat,
            ],
            rollout_strategy: Some(crate::models::RolloutStrategy::Canary),
            status: Some(crate::models::WorkStatus::Scheduled),
            priority: Some(90),
            progress_pct: Some(80),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec!["traceability".to_string()],
            plan: json!({"finding_key": finding.finding_key.clone()}),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        service
            .repository
            .upsert_work_item(&item)
            .await
            .expect("work item");

        let mut execution = WorkExecution::new(
            item.id,
            item.target_service.clone(),
            item.delivery_stage,
            item.rollout_strategy,
        );
        execution.status = crate::models::ExecutionStatus::Success;
        execution.refiner_job_id = Some("job-123".to_string());
        service
            .repository
            .upsert_work_execution(&execution)
            .await
            .expect("execution");
        service
            .upsert_work_item_link(
                item.id,
                NewTraceabilityLink {
                    execution_id: Some(execution.id),
                    finding_key: Some(finding.finding_key.clone()),
                    system: "tracey".to_string(),
                    reference_type: "rollout".to_string(),
                    reference_key: "tracey-canary".to_string(),
                    title: Some("Tracey canary rollout".to_string()),
                    status: Some("pending_rollback".to_string()),
                    url: Some("https://tracey.neuralmimicry.ai/status".to_string()),
                    metadata: json!({"source": "tracey"}),
                },
            )
            .await
            .expect("link");

        let app = build_router(service);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/traceability/graph")
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
        assert_eq!(payload["graph"]["node_totals"]["service"].as_u64(), Some(2));
        assert_eq!(
            payload["graph"]["node_totals"]["repository"].as_u64(),
            Some(2)
        );
        assert_eq!(
            payload["graph"]["relationship_totals"]["addresses_finding"].as_u64(),
            Some(1)
        );
        assert_eq!(
            payload["graph"]["relationship_totals"]["reported_by_service"].as_u64(),
            Some(1)
        );
        assert!(
            payload["graph"]["nodes"]
                .as_array()
                .expect("nodes")
                .iter()
                .any(|node| {
                    node["kind"].as_str() == Some("external_link")
                        && node["metadata"]["reference_key"].as_str() == Some("tracey-canary")
                })
        );
    }

    #[tokio::test]
    async fn atlassian_link_and_sync_endpoints_round_trip() {
        let (base_url, state, handle) = spawn_mock_atlassian().await;
        let service = test_service_with_atlassian(base_url);
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("atlassian:roundtrip".to_string()),
            title: "Publish Atlassian state".to_string(),
            summary: "Create Jira and Confluence links from Conductor.".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: None,
            priority: Some(70),
            progress_pct: Some(25),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec!["atlassian".to_string()],
            plan: json!({"finding_key": "repository_test_baseline:conductor"}),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        service
            .repository
            .upsert_work_item(&item)
            .await
            .expect("work item");

        let app = build_router(service);
        let jira = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/work-items/{}/links/jira", item.id))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"issue_type": "Bug"}).to_string()))
                    .unwrap(),
            )
            .await
            .expect("jira response");
        assert_eq!(jira.status(), StatusCode::OK);
        let jira_body = to_bytes(jira.into_body(), usize::MAX)
            .await
            .expect("jira body");
        let jira_payload: serde_json::Value =
            serde_json::from_slice(&jira_body).expect("jira json");
        assert_eq!(
            jira_payload["result"]["upstream_action"].as_str(),
            Some("created")
        );
        assert_eq!(
            jira_payload["result"]["link"]["reference_type"].as_str(),
            Some("bug")
        );

        let confluence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/work-items/{}/links/confluence", item.id))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .expect("confluence response");
        assert_eq!(confluence.status(), StatusCode::OK);
        let confluence_body = to_bytes(confluence.into_body(), usize::MAX)
            .await
            .expect("confluence body");
        let confluence_payload: serde_json::Value =
            serde_json::from_slice(&confluence_body).expect("confluence json");
        assert_eq!(
            confluence_payload["result"]["upstream_action"].as_str(),
            Some("created")
        );
        assert_eq!(
            confluence_payload["result"]["link"]["system"].as_str(),
            Some("confluence")
        );

        {
            let mut issue = state.issue.lock().await;
            let issue = issue.as_mut().expect("issue state");
            issue.status = "Done".to_string();
            issue.summary = "Resolved conductor bug".to_string();
        }
        {
            let mut page = state.page.lock().await;
            let page = page.as_mut().expect("page state");
            page.title = "Refreshed Conductor Work Item".to_string();
            page.version_number += 1;
        }

        let sync = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/work-items/{}/links/sync", item.id))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"systems": ["jira", "confluence"]}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("sync response");
        assert_eq!(sync.status(), StatusCode::OK);
        let sync_body = to_bytes(sync.into_body(), usize::MAX)
            .await
            .expect("sync body");
        let sync_payload: serde_json::Value =
            serde_json::from_slice(&sync_body).expect("sync json");
        assert_eq!(
            sync_payload["sync"]["links"]
                .as_array()
                .expect("links")
                .len(),
            2
        );
        assert!(
            sync_payload["sync"]["links"]
                .as_array()
                .expect("links")
                .iter()
                .any(|link| {
                    link["system"].as_str() == Some("jira")
                        && link["status"].as_str() == Some("Done")
                        && link["title"].as_str() == Some("Resolved conductor bug")
                })
        );
        assert!(
            sync_payload["sync"]["links"]
                .as_array()
                .expect("links")
                .iter()
                .any(|link| {
                    link["system"].as_str() == Some("confluence")
                        && link["title"].as_str() == Some("Refreshed Conductor Work Item")
                })
        );

        let traceability = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/work-items/{}/traceability", item.id))
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("traceability response");
        assert_eq!(traceability.status(), StatusCode::OK);
        let traceability_body = to_bytes(traceability.into_body(), usize::MAX)
            .await
            .expect("traceability body");
        let traceability_payload: serde_json::Value =
            serde_json::from_slice(&traceability_body).expect("traceability json");
        assert_eq!(
            traceability_payload["traceability"]["links"]
                .as_array()
                .expect("traceability links")
                .len(),
            2
        );
        assert!(
            traceability_payload["traceability"]["links"]
                .as_array()
                .expect("traceability links")
                .iter()
                .any(|link| {
                    link["system"].as_str() == Some("jira")
                        && link["status"].as_str() == Some("Done")
                })
        );

        handle.abort();
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
