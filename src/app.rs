use std::collections::BTreeSet;
use std::{convert::Infallible, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
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
        ConductorEvent, ConfluencePageLinkRequest, DeliveryStage, JiraIssueLinkRequest,
        NewResearchProject, NewTraceabilityLink, NewWorkItem, PublicResearchProject,
        ResearchProject, ResearchProjectPatch, ResearchProjectStatus, ResearchVisibility,
        TraceabilitySyncRequest, WorkExecution, WorkItem, WorkItemListFilter, WorkItemPatch,
        WorkStatus,
    },
    policy::{evaluate_work_item_with_repositories, policy_evaluation_to_value},
    service::ConductorService,
};

#[derive(Debug, Default, Deserialize)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct WorkItemsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub status: Option<WorkStatus>,
    pub target_service: Option<String>,
    pub search: Option<String>,
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
        .route("/metrics", get(metrics))
        .route("/dashboard", get(dashboard))
        .nest_service("/assets", ServeDir::new("assets"))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/host-recovery", get(host_recovery_status))
        .route("/api/v1/maintenance-windows", get(maintenance_windows))
        .route(
            "/api/v1/maintenance-windows/{proposal_id}/decision",
            post(decide_maintenance_window),
        )
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
            "/api/v1/work-items/{id}/policy-preview",
            get(preview_work_item_policy),
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
        .route(
            "/api/v1/research-projects",
            get(list_research_projects).post(create_research_project),
        )
        .route(
            "/api/v1/research-projects/{id}",
            get(get_research_project).patch(update_research_project),
        )
        .route(
            "/api/v1/research-projects/{id}/queue-research",
            post(queue_research_project),
        )
        .route(
            "/api/v1/public/research-projects",
            get(public_research_projects),
        )
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

async fn metrics() -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        crate::metrics::render_prometheus(),
    )
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

async fn host_recovery_status(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(service.host_recovery_status().await?))
}

#[derive(Debug, Deserialize)]
pub struct MaintenanceDecisionRequest {
    pub decision: String,
}

async fn maintenance_windows(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    Ok(Json(service.maintenance_windows().await?))
}

async fn decide_maintenance_window(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(proposal_id): Path<String>,
    Json(request): Json<MaintenanceDecisionRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    service.require_configured_admin(&headers)?;
    Ok(Json(
        service
            .decide_maintenance_window(&proposal_id, &request.decision)
            .await?,
    ))
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
    Query(query): Query<WorkItemsQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);
    if limit == 0 {
        return Err(ApiError::bad_request("limit must be at least 1"));
    }
    if offset > 1_000_000 {
        return Err(ApiError::bad_request("offset must not exceed 1000000"));
    }
    let target_service = query
        .target_service
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if target_service
        .as_ref()
        .is_some_and(|value| value.len() > 128)
    {
        return Err(ApiError::bad_request(
            "target_service must not exceed 128 bytes",
        ));
    }
    let search = query
        .search
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if search
        .as_ref()
        .is_some_and(|value| value.chars().count() > 200)
    {
        return Err(ApiError::bad_request(
            "search must not exceed 200 characters",
        ));
    }
    let limit = limit.min(500);
    let page = service
        .repository
        .list_work_items_page(&WorkItemListFilter {
            status: query.status,
            target_service,
            search,
            limit,
            offset,
        })
        .await?;
    Ok(Json(serde_json::json!({
        "work_items": page.items,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": page.total,
            "has_more": offset.saturating_add(limit) < page.total,
        }
    })))
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

async fn preview_work_item_policy(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    service.authorize_read(&headers)?;
    let item = service
        .work_item(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("work item {} not found", id)))?;
    let services = service.services().await?;
    let repositories = service.repositories().await?;
    let target_service = item.target_service.as_deref().and_then(|target| {
        services
            .iter()
            .find(|candidate| candidate.service_key == target)
    });
    let evaluation =
        evaluate_work_item_with_repositories(&service.config, &item, target_service, &repositories);

    Ok(Json(json!({
        "work_item_id": id,
        "read_only": true,
        "policy": policy_evaluation_to_value(&evaluation),
    })))
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

async fn list_research_projects(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    // Project briefs, resource bounds, budgets, review notes and sandbox
    // metadata are operationally sensitive even when general dashboard reads
    // are configured as public.
    service.require_configured_admin(&headers)?;
    let projects = service.repository.list_research_projects().await?;
    let projects = futures::future::try_join_all(
        projects
            .into_iter()
            .map(|project| refresh_research_project_progress(&service, project)),
    )
    .await?;
    Ok(Json(json!({"projects": projects})))
}

async fn get_research_project(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<ResearchProject>> {
    service.require_configured_admin(&headers)?;
    let project = service
        .repository
        .get_research_project(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("research project {id} not found")))?;
    let project = refresh_research_project_progress(&service, project).await?;
    Ok(Json(project))
}

/// Reconcile the visible project state with its newest Refiner work item.
/// Successful Refiner work is held at review rather than marked complete:
/// Conductor requires a human to assess the deliverables and publication gates.
async fn refresh_research_project_progress(
    service: &ConductorService,
    mut project: ResearchProject,
) -> ApiResult<ResearchProject> {
    if matches!(
        project.status,
        ResearchProjectStatus::Published
            | ResearchProjectStatus::Withdrawn
            | ResearchProjectStatus::Paused
            | ResearchProjectStatus::Completed
    ) {
        return Ok(project);
    }

    let mut latest = None;
    for work_item_id in &project.solver_work_item_ids {
        if let Some(item) = service.repository.get_work_item(*work_item_id).await?
            && latest
                .as_ref()
                .is_none_or(|current: &WorkItem| item.updated_at > current.updated_at)
        {
            latest = Some(item);
        }
    }
    let Some(item) = latest else {
        return Ok(project);
    };

    let (status, next_action, progress) = match item.status {
        WorkStatus::Planned | WorkStatus::Scheduled => (
            ResearchProjectStatus::Planning,
            "Review and approve the Refiner work item in the standard delivery queue.",
            item.progress_pct,
        ),
        WorkStatus::InOperation => (
            ResearchProjectStatus::Active,
            "Monitor the Refiner research run and review its evidence when it completes.",
            item.progress_pct,
        ),
        WorkStatus::Success => (
            ResearchProjectStatus::Review,
            "Review Refiner outputs, citations, methods, limitations and artifact provenance before accepting the project.",
            90,
        ),
        WorkStatus::Failure | WorkStatus::Aborted => (
            ResearchProjectStatus::Blocked,
            "Review the Refiner job logs, identify the cause and record a corrective action before retrying.",
            item.progress_pct,
        ),
        WorkStatus::OnHold => (
            ResearchProjectStatus::Blocked,
            "Resolve the Refiner work-item hold reason before resuming this research work.",
            item.progress_pct,
        ),
    };
    let progress = progress.clamp(0, 90);
    let next_action = next_action.to_string();
    if project.status != status
        || project.progress_pct < progress
        || project.next_action.as_deref() != Some(next_action.as_str())
    {
        project.status = status;
        project.progress_pct = project.progress_pct.max(progress);
        project.next_action = Some(next_action);
        project.updated_at = crate::models::now_utc();
        service.repository.upsert_research_project(&project).await?;
    }
    Ok(project)
}

async fn create_research_project(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Json(mut request): Json<NewResearchProject>,
) -> ApiResult<Json<ResearchProject>> {
    service.require_configured_admin(&headers)?;
    validate_project_text(&request.title, "title", 160)?;
    validate_project_text(&request.summary, "summary", 4_000)?;
    validate_project_text(&request.intent, "intent", 20_000)?;
    if request.owner.trim().is_empty() {
        request.owner = "Conductor".to_string();
    }
    validate_project_text(&request.owner, "owner", 160)?;
    request.domain = normalise_research_domain(&request.domain)?;
    if request
        .budget_limit_usdt
        .as_deref()
        .is_some_and(|value| !valid_nonnegative_decimal(value))
    {
        return Err(ApiError::bad_request(
            "budget_limit_usdt must be a non-negative decimal string",
        ));
    }
    if !request.resource_limits.is_null() && !request.resource_limits.is_object() {
        return Err(ApiError::bad_request(
            "resource_limits must be a JSON object",
        ));
    }
    if request.resource_limits.is_null() {
        request.resource_limits = json!({});
    }

    let id = Uuid::new_v4();
    let base_slug = slugify(&request.title);
    let slug = format!("{}-{}", base_slug, &id.to_string()[..8]);
    let project = ResearchProject::from_new(request, slug);
    service.repository.upsert_research_project(&project).await?;
    service.publish_event(ConductorEvent::new(
        "research.project.created",
        "A private research project was created for review and planning.",
        json!({"project_id": project.id, "slug": project.slug, "domain": project.domain}),
    ));
    Ok(Json(project))
}

async fn update_research_project(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(patch): Json<ResearchProjectPatch>,
) -> ApiResult<Json<ResearchProject>> {
    service.require_configured_admin(&headers)?;
    let mut project = service
        .repository
        .get_research_project(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("research project {id} not found")))?;

    if let Some(value) = patch.summary {
        validate_project_text(&value, "summary", 4_000)?;
        project.summary = value.trim().to_string();
    }
    if let Some(value) = patch.intent {
        validate_project_text(&value, "intent", 20_000)?;
        project.intent = value.trim().to_string();
    }
    if let Some(value) = patch.owner {
        validate_project_text(&value, "owner", 160)?;
        project.owner = value.trim().to_string();
    }
    if let Some(value) = patch.status {
        project.status = value;
    }
    if let Some(value) = patch.visibility {
        project.visibility = value;
    }
    if let Some(value) = patch.progress_pct {
        if !(0..=100).contains(&value) {
            return Err(ApiError::bad_request(
                "progress_pct must be between 0 and 100",
            ));
        }
        project.progress_pct = value;
    }
    if let Some(value) = patch.next_action {
        if value.len() > 1_000 {
            return Err(ApiError::bad_request("next_action exceeds 1000 characters"));
        }
        project.next_action = (!value.trim().is_empty()).then(|| value.trim().to_string());
    }
    if let Some(value) = patch.budget_limit_usdt {
        if !valid_nonnegative_decimal(&value) {
            return Err(ApiError::bad_request(
                "budget_limit_usdt must be a non-negative decimal string",
            ));
        }
        project.budget_limit_usdt = Some(value);
    }
    if let Some(value) = patch.budget_spent_usdt {
        if !valid_nonnegative_decimal(&value) {
            return Err(ApiError::bad_request(
                "budget_spent_usdt must be a non-negative decimal string",
            ));
        }
        project.budget_spent_usdt = Some(value);
    }
    if let Some(spent) = project.budget_spent_usdt.as_deref() {
        let Some(limit) = project.budget_limit_usdt.as_deref() else {
            return Err(ApiError::bad_request(
                "set an explicit budget_limit_usdt before recording project spend",
            ));
        };
        if decimal_exceeds(spent, limit) {
            return Err(ApiError::bad_request(
                "budget_spent_usdt cannot exceed budget_limit_usdt",
            ));
        }
    }
    if let Some(value) = patch.resource_limits {
        if !value.is_object() {
            return Err(ApiError::bad_request(
                "resource_limits must be a JSON object",
            ));
        }
        project.resource_limits = value;
    }
    if let Some(value) = patch.milestones {
        if value.len() > 200 {
            return Err(ApiError::bad_request(
                "a project may have at most 200 milestones",
            ));
        }
        project.milestones = value;
    }
    if let Some(value) = patch.evidence_urls {
        if value.len() > 500 || value.iter().any(|url| url.len() > 2_048) {
            return Err(ApiError::bad_request(
                "project evidence exceeds its size limit",
            ));
        }
        project.evidence_urls = value;
    }
    if let Some(value) = patch.fact_evidence {
        validate_research_fact_evidence(&value)?;
        project.fact_evidence = value;
    }
    if let Some(value) = patch.artifacts {
        validate_research_artifacts(&value)?;
        project.artifacts = value;
    }
    if let Some(value) = patch.review_gates {
        project.review_gates = value;
    }
    if let Some(value) = patch.sandbox {
        validate_research_sandbox(&value)?;
        project.sandbox = value;
    }

    if project.status == ResearchProjectStatus::Completed {
        let mut latest_refiner_work: Option<WorkItem> = None;
        for work_item_id in &project.solver_work_item_ids {
            if let Some(item) = service.repository.get_work_item(*work_item_id).await?
                && latest_refiner_work
                    .as_ref()
                    .is_none_or(|latest| item.updated_at > latest.updated_at)
            {
                latest_refiner_work = Some(item);
            }
        }
        if !latest_refiner_work.is_some_and(|item| item.status == WorkStatus::Success)
            || project.progress_pct != 100
            || !project.fact_evidence_ready()
            || !project.review_gates.publication_ready()
            || !project.artifacts.iter().any(|artifact| artifact.reviewed)
        {
            return Err(ApiError::bad_request(
                "completion requires successful Refiner work, 100% progress, reviewed output artefacts, a complete two-source-per-claim ledger and all independent review decisions",
            ));
        }
    }

    if project.status == ResearchProjectStatus::Published {
        project.visibility = ResearchVisibility::Public;
        if !project.public_page_ready()
            || !project.artifacts.iter().any(|artifact| {
                artifact.access == crate::models::ResearchArtifactAccess::Public
                    && artifact.reviewed
                    && artifact
                        .license
                        .as_deref()
                        .is_some_and(|license| !license.trim().is_empty())
            })
        {
            return Err(ApiError::bad_request(
                "publication requires every review gate, claim-by-claim evidence from at least two independent online sources, and at least one reviewed, licensed public artifact with an HTTPS URL",
            ));
        }
        if project.published_at.is_none() {
            project.published_at = Some(crate::models::now_utc());
        }
    } else {
        project.published_at = None;
    }

    project.updated_at = crate::models::now_utc();
    service.repository.upsert_research_project(&project).await?;
    service.publish_event(ConductorEvent::new(
        "research.project.updated",
        "Research project metadata or review state changed.",
        json!({
            "project_id": project.id,
            "slug": project.slug,
            "status": project.status.as_str(),
            "visibility": project.visibility.as_str(),
            "progress_pct": project.progress_pct,
        }),
    ));
    Ok(Json(project))
}

async fn queue_research_project(
    State(service): State<ConductorService>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    service.require_configured_admin(&headers)?;
    let mut project = service
        .repository
        .get_research_project(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("research project {id} not found")))?;
    if matches!(
        project.status,
        ResearchProjectStatus::Published
            | ResearchProjectStatus::Completed
            | ResearchProjectStatus::Paused
            | ResearchProjectStatus::Withdrawn
    ) {
        return Err(ApiError::bad_request(
            "paused, completed, published or withdrawn projects cannot start another research task",
        ));
    }
    if project.budget_limit_usdt.is_none() {
        return Err(ApiError::bad_request(
            "set an explicit USDT budget ceiling before queueing research; use 0 for a no-spend task",
        ));
    }
    if !project
        .resource_limits
        .as_object()
        .is_some_and(|limits| !limits.is_empty())
    {
        return Err(ApiError::bad_request(
            "set explicit resource limits before queueing research",
        ));
    }
    for work_item_id in &project.solver_work_item_ids {
        if let Some(item) = service.repository.get_work_item(*work_item_id).await?
            && matches!(
                item.status,
                WorkStatus::Planned | WorkStatus::Scheduled | WorkStatus::InOperation
            )
        {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "this project already has a non-terminal Refiner work item",
            ));
        }
    }

    let item = WorkItem::from_new(NewWorkItem {
        dedupe_key: Some(format!(
            "research-project:{}:{}",
            project.id,
            Uuid::new_v4()
        )),
        title: format!("Research: {}", project.title),
        summary: format!(
            "Evidence-led {} research project. {}",
            project.domain, project.summary
        ),
        target_service: Some("refiner".to_string()),
        delivery_stage: Some(DeliveryStage::Development),
        validated_stages: Vec::new(),
        rollout_strategy: None,
        status: Some(WorkStatus::Planned),
        priority: Some(50),
        progress_pct: Some(0),
        admin_override: false,
        execution_approved: false,
        verification_required: Some(true),
        tags: vec![
            "research".to_string(),
            "pure_research".to_string(),
            format!("domain:{}", project.domain),
        ],
        plan: json!({
            "research_project_id": project.id,
            "research_domain": project.domain,
            "research_brief": project.intent,
            "workflow": "project_solver",
            "research_requirements": {
                "source_citations_required": true,
                "minimum_independent_online_sources_per_factual_claim": 2,
                "citation_required_for_each_factual_claim": true,
                "record_claim_by_claim_source_matrix": true,
                "distinct_publishers_and_urls_required_per_claim": true,
                "if_two_independent_online_sources_cannot_be_verified": "mark the claim unverified and block completion; do not invent evidence",
                "methods_and_limitations_required": true,
                "do_not_modify_repositories": true,
                "do_not_access_production_systems": true,
                "outputs": ["research report", "claim-by-claim citation matrix", "sources", "methods", "limitations", "next steps"]
            },
            "ordered_steps": [
                "Clarify the research question, scope, assumptions, and success criteria.",
                "Use Refiner research functions to gather and cite reliable sources; distinguish primary evidence from interpretation.",
                "Analyse the evidence, document uncertainty and limitations, and check for contradictory findings.",
                "Produce an accessible report and a reproducible list of sources and methods for human review."
            ]
        }),
        depends_on: Vec::new(),
        source: Some("conductor_research_project".to_string()),
        scheduled_for: None,
    });
    service.repository.upsert_work_item(&item).await?;
    project.status = ResearchProjectStatus::Planning;
    project.progress_pct = project.progress_pct.max(5);
    project.next_action = Some(
        "Review and approve the Refiner work item in the standard Conductor delivery queue."
            .to_string(),
    );
    project.solver_work_item_ids.push(item.id);
    project.updated_at = crate::models::now_utc();
    service.repository.upsert_research_project(&project).await?;
    let mut event = ConductorEvent::new(
        "research.project.refiner_queued",
        "A Refiner research work item was created and awaits the standard approval gates.",
        json!({"project_id": project.id, "work_item_id": item.id, "workflow": "project_solver"}),
    );
    event.work_item_id = Some(item.id);
    service.publish_event(event);
    Ok(Json(json!({"project": project, "work_item": item})))
}

async fn public_research_projects(
    State(service): State<ConductorService>,
    headers: HeaderMap,
) -> ApiResult<(HeaderMap, Json<serde_json::Value>)> {
    let projects = service
        .repository
        .list_research_projects()
        .await?
        .into_iter()
        .filter(ResearchProject::public_page_ready)
        .map(|project| PublicResearchProject::from(&project))
        .collect::<Vec<_>>();
    let mut response_headers = HeaderMap::new();
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if ["https://neuralmimicry.ai", "https://www.neuralmimicry.ai"].contains(&origin) {
        response_headers.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            axum::http::HeaderValue::from_str(origin)
                .map_err(|error| ApiError::internal(error.to_string()))?,
        );
        response_headers.insert(header::VARY, axum::http::HeaderValue::from_static("Origin"));
    }
    Ok((response_headers, Json(json!({"projects": projects}))))
}

fn validate_project_text(value: &str, field: &str, max_len: usize) -> ApiResult<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request(format!("{field} must not be empty")));
    }
    if trimmed.len() > max_len {
        return Err(ApiError::bad_request(format!(
            "{field} exceeds {max_len} characters"
        )));
    }
    Ok(())
}

fn normalise_research_domain(value: &str) -> ApiResult<String> {
    let domain = value.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    const DOMAINS: &[&str] = &[
        "science",
        "mathematics",
        "humanities",
        "education",
        "languages",
        "law_ethics",
        "arts",
        "cross_disciplinary",
    ];
    if !DOMAINS.contains(&domain.as_str()) {
        return Err(ApiError::bad_request(
            "domain must be one of science, mathematics, humanities, education, languages, law_ethics, arts, or cross_disciplinary",
        ));
    }
    Ok(domain)
}

fn valid_nonnegative_decimal(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() || value.len() > 38 {
        return false;
    }
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    !whole.is_empty()
        && whole.len() <= 29
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && fraction.is_none_or(|digits| {
            !digits.is_empty()
                && digits.len() <= 8
                && digits.bytes().all(|byte| byte.is_ascii_digit())
        })
        && parts.next().is_none()
}

fn decimal_exceeds(value: &str, limit: &str) -> bool {
    fn units(value: &str) -> Option<i128> {
        if !valid_nonnegative_decimal(value) {
            return None;
        }
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        let whole: i128 = whole.parse().ok()?;
        let fraction = format!("{fraction:0<8}");
        let fraction: i128 = fraction.parse().ok()?;
        whole.checked_mul(100_000_000)?.checked_add(fraction)
    }
    matches!((units(value), units(limit)), (Some(value), Some(limit)) if value > limit)
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            if separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(character);
            separator = false;
        } else {
            separator = true;
        }
        if slug.len() >= 56 {
            break;
        }
    }
    if slug.is_empty() {
        "research-project".to_string()
    } else {
        slug.trim_end_matches('-').to_string()
    }
}

fn validate_research_artifacts(artifacts: &[crate::models::ResearchArtifact]) -> ApiResult<()> {
    if artifacts.len() > 100 {
        return Err(ApiError::bad_request(
            "a project may have at most 100 artifacts",
        ));
    }
    for artifact in artifacts {
        validate_project_text(&artifact.title, "artifact title", 240)?;
        validate_project_text(&artifact.media_type, "artifact media_type", 120)?;
        validate_project_text(&artifact.provenance, "artifact provenance", 4_000)?;
        let parsed_url = reqwest::Url::parse(&artifact.url).ok();
        if artifact.url.len() > 2_048
            || !parsed_url.as_ref().is_some_and(|url| {
                url.scheme() == "https" && url.host_str().is_some_and(|host| !host.is_empty())
            })
        {
            return Err(ApiError::bad_request(
                "artifact URLs must use HTTPS and be at most 2048 characters",
            ));
        }
        if artifact.access == crate::models::ResearchArtifactAccess::Public
            && parsed_url.as_ref().is_some_and(|url| {
                !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
            })
        {
            return Err(ApiError::bad_request(
                "public artifact URLs cannot contain credentials, query strings, or fragments",
            ));
        }
        if artifact.sha256.as_deref().is_some_and(|digest| {
            digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) {
            return Err(ApiError::bad_request(
                "artifact sha256 must contain 64 hexadecimal characters",
            ));
        }
        if artifact
            .license
            .as_deref()
            .is_some_and(|license| license.len() > 240)
        {
            return Err(ApiError::bad_request(
                "artifact license exceeds 240 characters",
            ));
        }
    }
    Ok(())
}

fn validate_research_fact_evidence(facts: &[crate::models::ResearchFactEvidence]) -> ApiResult<()> {
    if facts.len() > 1_000 {
        return Err(ApiError::bad_request(
            "a project may record at most 1000 factual claims",
        ));
    }
    for fact in facts {
        validate_project_text(&fact.claim, "factual claim", 5_000)?;
        if !(2..=20).contains(&fact.citations.len()) {
            return Err(ApiError::bad_request(
                "each factual claim must cite between 2 and 20 independent online sources",
            ));
        }
        let mut publishers = BTreeSet::new();
        let mut urls = BTreeSet::new();
        for citation in &fact.citations {
            validate_project_text(&citation.title, "citation title", 500)?;
            validate_project_text(&citation.publisher, "citation publisher", 240)?;
            let url = reqwest::Url::parse(&citation.url).ok();
            if citation.url.len() > 2_048
                || !url.as_ref().is_some_and(|url| {
                    url.scheme() == "https"
                        && url.host_str().is_some_and(|host| !host.is_empty())
                        && url.username().is_empty()
                        && url.password().is_none()
                })
            {
                return Err(ApiError::bad_request(
                    "research citations must use valid HTTPS URLs without embedded credentials",
                ));
            }
            if citation.accessed_at > crate::models::now_utc() {
                return Err(ApiError::bad_request(
                    "citation accessed_at must not be in the future",
                ));
            }
            let normalised_url = url
                .as_ref()
                .map(|url| url.as_str().to_ascii_lowercase())
                .unwrap_or_default();
            if !publishers.insert(citation.publisher.trim().to_ascii_lowercase())
                || !urls.insert(normalised_url)
            {
                return Err(ApiError::bad_request(
                    "each factual claim requires distinct source publishers and URLs",
                ));
            }
        }
    }
    Ok(())
}

fn validate_research_sandbox(sandbox: &crate::models::ResearchSandbox) -> ApiResult<()> {
    const STATUSES: &[&str] = &[
        "not_requested",
        "requested",
        "provisioning",
        "active",
        "tearing_down",
        "complete",
        "failed",
        "cleanup_required",
    ];
    if !STATUSES.contains(&sandbox.status.as_str()) {
        return Err(ApiError::bad_request("sandbox status is not supported"));
    }
    if sandbox
        .deployment_id
        .as_deref()
        .is_some_and(|id| id.len() > 160)
    {
        return Err(ApiError::bad_request(
            "sandbox deployment_id exceeds 160 characters",
        ));
    }
    if !sandbox.resource_manifest.is_null() && !sandbox.resource_manifest.is_object() {
        return Err(ApiError::bad_request(
            "sandbox resource_manifest must be a JSON object",
        ));
    }
    if sandbox.retained_data.len() > 100
        || sandbox.retained_data.iter().any(|path| path.len() > 500)
    {
        return Err(ApiError::bad_request(
            "sandbox retained_data exceeds its size limit",
        ));
    }
    Ok(())
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
    let force_schedule = request.force_schedule.unwrap_or(false);
    let execution = service
        .start_work_item_execution(id, force_schedule)
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
        config::ConductorConfig, integrations::build_http_client, repository::ConductorRepository,
        service::ConductorService, storage::memory::MemoryRepository,
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
    async fn host_recovery_status_requires_auth_when_token_is_configured() {
        let response = build_router(test_service())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/host-recovery")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("unauthorised response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = build_router(test_service())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/host-recovery")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("authorised response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("status body");
        let status: serde_json::Value = serde_json::from_slice(&body).expect("status JSON");
        assert_eq!(status["automatic_loop_enabled"], false);
        assert_eq!(status["dry_run"], true);
    }

    #[tokio::test]
    async fn maintenance_windows_require_auth_and_show_shared_chassis_pairs() {
        let response = build_router(test_service())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/maintenance-windows")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("unauthorised response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = build_router(test_service())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/maintenance-windows")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("authorised response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("maintenance status body");
        let status: serde_json::Value = serde_json::from_slice(&body).expect("status JSON");
        assert_eq!(status["chassis"].as_array().expect("chassis").len(), 3);
        assert_eq!(status["chassis"][2]["device_ids"], json!(["qc04", "qc05"]));
        assert_eq!(status["evacuation_automation_available"], false);
    }

    #[tokio::test]
    async fn accepted_maintenance_decision_is_persisted_but_does_not_start_evacuations() {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        let repository = Arc::new(MemoryRepository::new());
        let proposal_id = "centriq-qc04-qc05-0123456789abcdef";
        let suggested_start = now_utc() + chrono::Duration::days(8);
        let mut proposal = crate::models::ConductorEvent::new(
            "maintenance.window.proposed",
            "qc04 battery service proposal",
            json!({
                "proposal_id": proposal_id,
                "chassis_id": "centriq-qc04-qc05",
                "suggested_window_start": suggested_start,
                "issue_generation": "episode-1",
                "affected_devices": ["qc04"],
            }),
        );
        proposal.status = Some("suggested".to_string());
        repository
            .insert_conductor_event(&proposal)
            .await
            .expect("proposal event");
        repository
            .upsert_maintenance_observation(&crate::models::MaintenanceObservation {
                chassis_id: "centriq-qc04-qc05".to_string(),
                observed_at: now_utc(),
                payload: json!({
                    "issue_state": "active",
                    "issue_generation": "episode-1",
                    "all_members_fresh": true,
                    "affected_devices": ["qc04"],
                    "critical_sensors": ["SYS_V3_Battery"],
                }),
            })
            .await
            .expect("current BMC observation");
        let service = ConductorService::new(
            config,
            repository.clone(),
            build_http_client(2).expect("HTTP client"),
        );
        let app = build_router(service);
        let request = || {
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/maintenance-windows/{proposal_id}/decision"
                ))
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"decision":"accept"}"#))
                .unwrap()
        };
        let response = app
            .clone()
            .oneshot(request())
            .await
            .expect("accepted decision response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("decision body");
        let result: serde_json::Value = serde_json::from_slice(&body).expect("decision JSON");
        assert_eq!(result["status"], "accepted_blocked");
        assert_eq!(result["event"]["payload"]["preparation_status"], "blocked");
        assert_eq!(
            repository
                .list_conductor_events(10)
                .await
                .expect("persisted events")
                .iter()
                .filter(|event| event.event_type == "maintenance.window.decision")
                .count(),
            1
        );

        let response = app
            .oneshot(request())
            .await
            .expect("duplicate decision response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
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
    async fn dashboard_includes_neuralmimicry_header_and_version() {
        let app = build_router(test_service());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("dashboard body");
        let html = String::from_utf8(body.to_vec()).expect("utf-8 html");
        assert!(html.contains("NeuralMimicry"));
        assert!(html.contains("/assets/neuralmimicryicon512.png"));
        assert!(html.contains(&format!("Conductor v{}", env!("CARGO_PKG_VERSION"))));
    }

    #[tokio::test]
    async fn private_research_project_reads_require_admin_even_when_dashboard_reads_are_public() {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        config.security.allow_dashboard_without_token = true;
        let repo = Arc::new(MemoryRepository::new());
        let app = build_router(ConductorService::new(
            config,
            repo,
            build_http_client(2).expect("client"),
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/research-projects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn private_research_routes_fail_closed_when_admin_token_is_unconfigured() {
        let mut config = ConductorConfig::default();
        config.security.allow_dashboard_without_token = true;
        let repo = Arc::new(MemoryRepository::new());
        let app = build_router(ConductorService::new(
            config,
            repo,
            build_http_client(2).expect("client"),
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/research-projects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn research_queue_creates_reviewable_refiner_work_without_auto_approval() {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        let repo = Arc::new(MemoryRepository::new());
        let app = build_router(ConductorService::new(
            config,
            repo.clone(),
            build_http_client(2).expect("client"),
        ));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/research-projects")
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "title": "Accessible science lessons",
                            "summary": "Evidence-led research into accessible lesson formats.",
                            "intent": "Compare learning outcomes and accessibility across formats.",
                            "domain": "education",
                            "owner": "Conductor",
                            "budget_limit_usdt": "0",
                            "resource_limits": {"network": "research-only", "max_cost_usdt": "0"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("create body");
        let created: ResearchProject = serde_json::from_slice(&body).expect("created project");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/research-projects/{}/queue-research",
                        created.id
                    ))
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("queue response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("queue body");
        let queued: serde_json::Value = serde_json::from_slice(&body).expect("queued payload");
        assert_eq!(queued["work_item"]["target_service"], "refiner");
        assert_eq!(queued["work_item"]["source"], "conductor_research_project");
        assert_eq!(queued["work_item"]["execution_approved"], false);
        assert_eq!(queued["work_item"]["verification_required"], true);
        assert_eq!(queued["work_item"]["plan"]["workflow"], "project_solver");
        assert_eq!(
            queued["work_item"]["plan"]["research_requirements"]["do_not_access_production_systems"],
            true
        );
        assert_eq!(
            queued["work_item"]["plan"]["research_requirements"]["minimum_independent_online_sources_per_factual_claim"],
            2
        );
        assert_eq!(
            queued["work_item"]["plan"]["research_requirements"]["citation_required_for_each_factual_claim"],
            true
        );
        assert_eq!(repo.list_work_items().await.expect("work items").len(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/research-projects/{}/queue-research",
                        created.id
                    ))
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("duplicate queue response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(repo.list_work_items().await.expect("work items").len(), 1);

        let work_item = repo.list_work_items().await.expect("work items").remove(0);
        repo.patch_work_item(
            work_item.id,
            crate::models::WorkItemPatch {
                status: Some(WorkStatus::Success),
                progress_pct: Some(100),
                ..crate::models::WorkItemPatch::default()
            },
        )
        .await
        .expect("complete Refiner work item");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/research-projects")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("refreshed project response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("refreshed project body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("projects");
        assert_eq!(payload["projects"][0]["status"], "review");
        assert_eq!(payload["projects"][0]["progress_pct"], 90);
        assert!(
            payload["projects"][0]["next_action"]
                .as_str()
                .expect("next action")
                .contains("Review Refiner outputs")
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/v1/research-projects/{}", created.id))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"status": "completed", "progress_pct": 100}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("premature completion response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn public_research_list_excludes_unreviewed_or_unusable_projects() {
        let config = ConductorConfig::default();
        let repo = Arc::new(MemoryRepository::new());
        let mut ready = ResearchProject::from_new(
            NewResearchProject {
                title: "Reviewed public project".to_string(),
                summary: "A reviewed project summary.".to_string(),
                intent: "A sufficiently detailed research intent.".to_string(),
                domain: "science".to_string(),
                owner: "Conductor".to_string(),
                budget_limit_usdt: None,
                resource_limits: json!({}),
            },
            "reviewed-public-project-test".to_string(),
        );
        let now = now_utc();
        ready.status = ResearchProjectStatus::Published;
        ready.visibility = ResearchVisibility::Public;
        ready.published_at = Some(now);
        let decision = |reviewer: &str| crate::models::ResearchReviewDecision {
            approved: true,
            reviewer: Some(reviewer.to_string()),
            reviewed_at: Some(now),
            evidence_url: None,
        };
        ready.review_gates = crate::models::ResearchReviewGates {
            subject: decision("subject reviewer"),
            quality: decision("quality reviewer"),
            evidence: decision("evidence reviewer"),
            privacy: decision("privacy reviewer"),
            safety: decision("safety reviewer"),
            licensing: decision("licensing reviewer"),
            accessibility: decision("accessibility reviewer"),
        };
        ready.artifacts.push(crate::models::ResearchArtifact {
            id: Uuid::new_v4(),
            title: "Public report".to_string(),
            media_type: "report".to_string(),
            url: "https://research.example.org/report".to_string(),
            access: crate::models::ResearchArtifactAccess::Public,
            license: Some("CC-BY-4.0".to_string()),
            sha256: None,
            provenance: "Reviewed report generated from cited public sources.".to_string(),
            reviewed: true,
            created_at: now,
        });
        ready
            .fact_evidence
            .push(crate::models::ResearchFactEvidence {
                claim: "The example finding is supported by two independent online sources."
                    .to_string(),
                citations: vec![
                    crate::models::ResearchCitationSource {
                        url: "https://journal.example.org/article".to_string(),
                        title: "Primary study".to_string(),
                        publisher: "Example Journal".to_string(),
                        accessed_at: now,
                    },
                    crate::models::ResearchCitationSource {
                        url: "https://institute.example.net/briefing".to_string(),
                        title: "Independent evidence review".to_string(),
                        publisher: "Example Institute".to_string(),
                        accessed_at: now,
                    },
                ],
            });
        repo.upsert_research_project(&ready)
            .await
            .expect("ready project");

        let mut empty = ready.clone();
        empty.id = Uuid::new_v4();
        empty.slug = "published-without-artifacts-test".to_string();
        empty.title = "Published but unusable".to_string();
        empty.artifacts.clear();
        repo.upsert_research_project(&empty)
            .await
            .expect("empty project");

        let mut private = ready.clone();
        private.id = Uuid::new_v4();
        private.slug = "private-project-test".to_string();
        private.title = "Private project".to_string();
        private.visibility = ResearchVisibility::Private;
        repo.upsert_research_project(&private)
            .await
            .expect("private project");

        let app = build_router(ConductorService::new(
            config,
            repo,
            build_http_client(2).expect("client"),
        ));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/public/research-projects")
                    .header("origin", "https://neuralmimicry.ai")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("https://neuralmimicry.ai")
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("public body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("public projects");
        assert_eq!(payload["projects"].as_array().expect("array").len(), 1);
        assert_eq!(
            payload["projects"][0]["slug"],
            "reviewed-public-project-test"
        );
        assert_eq!(
            payload["projects"][0]["fact_evidence"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/public/research-projects")
                    .header("origin", "https://untrusted.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("untrusted origin response");
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
    }

    #[test]
    fn factual_claims_require_two_distinct_online_publishers() {
        let now = now_utc();
        let citation = |url: &str, publisher: &str| crate::models::ResearchCitationSource {
            url: url.to_string(),
            title: "Evidence source".to_string(),
            publisher: publisher.to_string(),
            accessed_at: now,
        };
        let insufficient = vec![crate::models::ResearchFactEvidence {
            claim: "A factual research claim.".to_string(),
            citations: vec![citation("https://one.example.org/source", "Publisher One")],
        }];
        assert!(validate_research_fact_evidence(&insufficient).is_err());

        let same_publisher = vec![crate::models::ResearchFactEvidence {
            claim: "A factual research claim.".to_string(),
            citations: vec![
                citation("https://one.example.org/source", "Publisher One"),
                citation("https://one.example.org/another", " publisher one "),
            ],
        }];
        assert!(validate_research_fact_evidence(&same_publisher).is_err());

        let duplicate_url = vec![crate::models::ResearchFactEvidence {
            claim: "A factual research claim.".to_string(),
            citations: vec![
                citation("https://one.example.org/source", "Publisher One"),
                citation("https://one.example.org/source", "Publisher Two"),
            ],
        }];
        assert!(validate_research_fact_evidence(&duplicate_url).is_err());
    }

    #[test]
    fn budget_comparison_uses_decimal_units_without_float_rounding() {
        assert!(!decimal_exceeds("1.23000000", "1.23"));
        assert!(!decimal_exceeds("0002.4", "2.40000000"));
        assert!(decimal_exceeds("1.00000001", "1"));
        assert!(decimal_exceeds("10", "9.99999999"));
        assert!(!decimal_exceeds(
            "99999999999999999999999999998",
            "99999999999999999999999999999"
        ));
    }

    #[tokio::test]
    async fn publication_is_rejected_without_a_complete_two_source_claim_matrix() {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        let repo = Arc::new(MemoryRepository::new());
        let mut project = ResearchProject::from_new(
            NewResearchProject {
                title: "Evidence gate test".to_string(),
                summary: "Checks the research publication evidence gate.".to_string(),
                intent: "Confirm publication cannot omit factual source evidence.".to_string(),
                domain: "science".to_string(),
                owner: "Conductor".to_string(),
                budget_limit_usdt: Some("0".to_string()),
                resource_limits: json!({}),
            },
            "evidence-gate-test".to_string(),
        );
        let now = now_utc();
        let decision = |reviewer: &str| crate::models::ResearchReviewDecision {
            approved: true,
            reviewer: Some(reviewer.to_string()),
            reviewed_at: Some(now),
            evidence_url: None,
        };
        project.review_gates = crate::models::ResearchReviewGates {
            subject: decision("subject reviewer"),
            quality: decision("quality reviewer"),
            evidence: decision("evidence reviewer"),
            privacy: decision("privacy reviewer"),
            safety: decision("safety reviewer"),
            licensing: decision("licensing reviewer"),
            accessibility: decision("accessibility reviewer"),
        };
        project.artifacts.push(crate::models::ResearchArtifact {
            id: Uuid::new_v4(),
            title: "Reviewed report".to_string(),
            media_type: "report".to_string(),
            url: "https://research.example.org/report".to_string(),
            access: crate::models::ResearchArtifactAccess::Public,
            license: Some("CC-BY-4.0".to_string()),
            sha256: None,
            provenance: "Report fixture for publication-gate testing.".to_string(),
            reviewed: true,
            created_at: now,
        });
        repo.upsert_research_project(&project)
            .await
            .expect("project fixture");
        let app = build_router(ConductorService::new(
            config,
            repo.clone(),
            build_http_client(2).expect("client"),
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/v1/research-projects/{}", project.id))
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"status": "published", "visibility": "public"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("publication response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("publication error body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("error payload");
        assert!(
            payload["error"]
                .as_str()
                .expect("error text")
                .contains("claim-by-claim evidence")
        );
        assert_eq!(
            repo.get_research_project(project.id)
                .await
                .expect("stored project")
                .expect("project")
                .status,
            ResearchProjectStatus::Proposed
        );
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
    async fn manual_procurement_policy_preview_is_blocked_and_read_only() {
        let mut config = ConductorConfig::default();
        config.security.admin_token = Some("secret".to_string());
        config.security.allow_dashboard_without_token = false;
        let repository = std::sync::Arc::new(MemoryRepository::new());
        let service = ConductorService::new(
            config,
            repository.clone(),
            build_http_client(2).expect("client"),
        );
        let item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("manual-procurement:policy-preview".to_string()),
            title: "Review supplier request".to_string(),
            summary: "A manually reviewed purchase request".to_string(),
            target_service: Some("refiner".to_string()),
            delivery_stage: None,
            validated_stages: vec![],
            rollout_strategy: None,
            status: Some(WorkStatus::Planned),
            priority: None,
            progress_pct: None,
            admin_override: false,
            execution_approved: false,
            verification_required: None,
            tags: vec!["manual-procurement".to_string()],
            plan: json!({"action": "review supplier request"}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        repository
            .upsert_work_item(&item)
            .await
            .expect("seed manual procurement item");

        let response = build_router(service)
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/work-items/{}/policy-preview", item.id))
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
        assert_eq!(payload["read_only"], true);
        assert_eq!(payload["policy"]["verdict"], "blocked");
        assert!(
            payload["policy"]["reasons"]
                .as_array()
                .expect("policy reasons")
                .iter()
                .any(|reason| reason.as_str().is_some_and(
                    |value| value.contains("blocked action keyword 'manual-procurement'")
                ))
        );

        let persisted = repository
            .get_work_item(item.id)
            .await
            .expect("read persisted work item")
            .expect("work item remains present");
        assert!(!persisted.execution_approved);
        assert_eq!(persisted.last_policy, json!({}));
        assert!(
            repository
                .list_work_executions_for_item(item.id, 10)
                .await
                .expect("read executions")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn work_items_endpoint_filters_and_returns_a_bounded_page() {
        let service = test_service();
        for (title, status, target_service) in [
            ("Solver one", WorkStatus::Planned, Some("refiner")),
            ("Solver two", WorkStatus::Planned, Some("refiner")),
            ("Solver three", WorkStatus::Planned, Some("refiner")),
            ("Other work", WorkStatus::Planned, Some("conductor")),
            ("Completed solver", WorkStatus::Success, Some("refiner")),
        ] {
            let item = WorkItem::from_new(NewWorkItem {
                dedupe_key: None,
                title: title.to_string(),
                summary: "Pagination test item".to_string(),
                target_service: target_service.map(str::to_string),
                delivery_stage: None,
                validated_stages: vec![],
                rollout_strategy: None,
                status: Some(status),
                priority: None,
                progress_pct: None,
                admin_override: false,
                execution_approved: false,
                verification_required: None,
                tags: vec![],
                plan: json!({}),
                depends_on: vec![],
                source: None,
                scheduled_for: None,
            });
            service
                .repository
                .upsert_work_item(&item)
                .await
                .expect("seed work item");
        }

        let response = build_router(service)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/work-items?limit=1&offset=1&status=planned&target_service=refiner&search=solver")
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
        assert_eq!(payload["pagination"]["limit"], 1);
        assert_eq!(payload["pagination"]["offset"], 1);
        assert_eq!(payload["pagination"]["total"], 3);
        assert_eq!(payload["pagination"]["has_more"], true);
        let items = payload["work_items"].as_array().expect("work item page");
        assert_eq!(items.len(), 1);
        assert!(items[0]["title"].as_str().unwrap().contains("Solver"));
    }

    #[tokio::test]
    async fn work_items_endpoint_rejects_zero_limit() {
        let response = build_router(test_service())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/work-items?limit=0")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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
