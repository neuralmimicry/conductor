use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use axum::http::HeaderMap;
use chrono::Duration as ChronoDuration;
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    config::ConductorConfig,
    discovery::discover_and_probe,
    error::ApiError,
    executor::{ExecutionEventCallback, execute_specific_work_item, run_execution_cycle},
    integrations::{atlassian::AtlassianClients, refiner::RefinerClient, tracey::TraceyClient},
    models::{
        ConductorEvent, ConfluencePageLinkRequest, DashboardSummary, DeliveryStage, DiscoveryRun,
        DoraMetricsSummary, ExternalLinkOperationResult, FindingEvidence, FindingProvenance,
        FindingRecord, ImprovementCycle, JiraIssueLinkRequest, NewTraceabilityLink,
        RepositorySnapshot, ServiceSnapshot, TraceabilityGraph, TraceabilityGraphEdge,
        TraceabilityGraphNode, TraceabilityLink, TraceabilitySyncRequest, TraceabilitySyncResult,
        WorkExecution, WorkItem, WorkItemTraceability, WorkStatus, now_utc, topology_from_services,
        unique_strings,
    },
    planner::run_planning_cycle,
    repository::ConductorRepository,
    trends::collect_metric_samples,
};

#[derive(Clone)]
pub struct ConductorService {
    pub config: Arc<ConductorConfig>,
    pub repository: Arc<dyn ConductorRepository>,
    pub http: Client,
    pub events: broadcast::Sender<ConductorEvent>,
}

impl ConductorService {
    pub fn new(
        config: ConductorConfig,
        repository: Arc<dyn ConductorRepository>,
        http: Client,
    ) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            config: Arc::new(config),
            repository,
            http,
            events,
        }
    }

    pub fn publish_event(&self, event: ConductorEvent) {
        let _ = self.events.send(event.clone());
        persist_conductor_event_async(self.repository.clone(), event);
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ConductorEvent> {
        self.events.subscribe()
    }

    fn event_callback(&self) -> ExecutionEventCallback {
        let sender = self.events.clone();
        let repository = self.repository.clone();
        Arc::new(move |event: ConductorEvent| {
            let _ = sender.send(event.clone());
            persist_conductor_event_async(repository.clone(), event);
        })
    }

    pub fn authorize_read(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        self.authorize_read_with_token(headers, None)
    }

    pub fn authorize_read_with_token(
        &self,
        headers: &HeaderMap,
        token_override: Option<&str>,
    ) -> Result<(), ApiError> {
        if self.config.security.allow_dashboard_without_token {
            return Ok(());
        }
        self.authorize_admin_with_token(headers, token_override)
    }

    pub fn authorize_admin(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        self.authorize_admin_with_token(headers, None)
    }

    pub fn authorize_admin_with_token(
        &self,
        headers: &HeaderMap,
        token_override: Option<&str>,
    ) -> Result<(), ApiError> {
        let Some(expected) = self
            .config
            .security
            .admin_token
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(());
        };
        let supplied = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim);
        let supplied = supplied.or_else(|| token_override.map(str::trim));
        match supplied {
            Some(token) if token == expected => Ok(()),
            _ => Err(ApiError::unauthorized()),
        }
    }

    pub async fn run_discovery_cycle(&self) -> Result<DiscoveryRun> {
        let discovery = discover_and_probe(&self.config, &self.http).await?;
        let metric_samples = collect_metric_samples(discovery.run.id, &discovery.services);
        self.repository
            .replace_service_snapshots(&discovery.services)
            .await?;
        self.repository
            .replace_repository_snapshots(&discovery.repositories)
            .await?;
        self.repository.insert_discovery_run(&discovery.run).await?;
        if !metric_samples.is_empty() {
            self.repository
                .insert_service_metric_samples(&metric_samples)
                .await?;
        }
        let mut event = ConductorEvent::new(
            "discovery.completed",
            format!(
                "discovery cycle completed with {} services and {} repositories",
                discovery.services.len(),
                discovery.repositories.len()
            ),
            json!({
                "discovery_run_id": discovery.run.id.to_string(),
                "services_count": discovery.services.len(),
                "repositories_count": discovery.repositories.len(),
                "status": discovery.run.status.as_str(),
            }),
        );
        event.status = Some(discovery.run.status.as_str().to_string());
        self.publish_event(event);
        Ok(discovery.run)
    }

    pub async fn run_planning_cycle(&self) -> Result<ImprovementCycle> {
        let cycle = run_planning_cycle(self.repository.as_ref(), &self.http, &self.config).await?;
        let mut event = ConductorEvent::new(
            "planning.completed",
            cycle.summary.clone(),
            json!({
                "improvement_cycle_id": cycle.id.to_string(),
                "status": cycle.status.as_str(),
                "source_services": cycle.source_services.clone(),
            }),
        );
        event.status = Some(cycle.status.as_str().to_string());
        self.publish_event(event);
        Ok(cycle)
    }

    pub async fn services(&self) -> Result<Vec<crate::models::ServiceSnapshot>> {
        self.repository.list_service_snapshots().await
    }

    pub async fn repositories(&self) -> Result<Vec<RepositorySnapshot>> {
        self.repository.list_repository_snapshots().await
    }

    pub async fn findings(&self) -> Result<Vec<FindingRecord>> {
        self.repository.list_findings().await
    }

    pub async fn finding(&self, id: Uuid) -> Result<Option<FindingRecord>> {
        self.repository.get_finding(id).await
    }

    pub async fn finding_evidence(&self, id: Uuid) -> Result<Vec<FindingEvidence>> {
        self.repository.list_finding_evidence(id).await
    }

    pub async fn finding_provenance(&self, id: Uuid) -> Result<Vec<FindingProvenance>> {
        self.repository.list_finding_provenance(id).await
    }

    pub async fn work_items(&self) -> Result<Vec<WorkItem>> {
        self.repository.list_work_items().await
    }

    pub async fn work_item(&self, id: Uuid) -> Result<Option<WorkItem>> {
        self.repository.get_work_item(id).await
    }

    pub async fn executions(&self, limit: usize) -> Result<Vec<WorkExecution>> {
        self.repository.list_work_executions(limit).await
    }

    pub async fn events(&self, limit: usize) -> Result<Vec<ConductorEvent>> {
        self.repository.list_conductor_events(limit).await
    }

    pub async fn work_item_executions(
        &self,
        work_item_id: Uuid,
        limit: usize,
    ) -> Result<Vec<WorkExecution>> {
        self.repository
            .list_work_executions_for_item(work_item_id, limit)
            .await
    }

    pub async fn work_item_links(
        &self,
        work_item_id: Uuid,
        limit: usize,
    ) -> Result<Vec<TraceabilityLink>> {
        self.repository
            .list_traceability_links(Some(work_item_id), None, None, limit)
            .await
    }

    pub async fn upsert_work_item_link(
        &self,
        work_item_id: Uuid,
        mut request: NewTraceabilityLink,
    ) -> Result<TraceabilityLink> {
        let work_item = self
            .repository
            .get_work_item(work_item_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("work item {} not found", work_item_id))?;
        if request.finding_key.is_none() {
            request.finding_key = work_item
                .plan
                .get("finding_key")
                .and_then(|value| value.as_str())
                .map(ToString::to_string);
        }
        let link = TraceabilityLink::from_new(Some(work_item_id), request);
        self.repository.upsert_traceability_link(&link).await?;

        let stored = self
            .repository
            .list_traceability_links(
                Some(work_item_id),
                link.execution_id,
                link.finding_key.as_deref(),
                200,
            )
            .await?
            .into_iter()
            .find(|candidate| candidate.link_key == link.link_key)
            .unwrap_or(link);

        let mut event = ConductorEvent::new(
            "traceability.linked",
            format!(
                "linked {} {} to work item {}",
                stored.system, stored.reference_key, work_item_id
            ),
            json!({
                "work_item_id": work_item_id.to_string(),
                "link_key": stored.link_key.clone(),
                "system": stored.system.clone(),
                "reference_type": stored.reference_type.clone(),
                "reference_key": stored.reference_key.clone(),
            }),
        );
        event.work_item_id = Some(work_item_id);
        event.execution_id = stored.execution_id;
        event.status = stored.status.clone();
        self.publish_event(event);

        Ok(stored)
    }

    pub async fn link_work_item_jira(
        &self,
        work_item_id: Uuid,
        request: JiraIssueLinkRequest,
    ) -> Result<ExternalLinkOperationResult> {
        let traceability = self
            .work_item_traceability(work_item_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("work item {} not found", work_item_id))?;
        let JiraIssueLinkRequest {
            execution_id,
            finding_key,
            issue_key,
            project_key,
            issue_type,
            reference_type,
            summary,
            description,
            labels,
            update_existing,
            transition_name,
            fields,
        } = request;
        let resolved_execution_id =
            execution_id.or_else(|| traceability.latest_execution.as_ref().map(|item| item.id));
        let resolved_finding_key = resolve_finding_key(finding_key, &traceability);
        let project_key = project_key
            .or_else(|| self.config.integrations.atlassian.jira_project_key.clone())
            .ok_or_else(|| anyhow::anyhow!("atlassian jira_project_key is not configured"))?;
        let issue_type = issue_type
            .unwrap_or_else(|| self.config.integrations.atlassian.jira_issue_type.clone());
        let reference_type = reference_type.unwrap_or_else(|| jira_reference_type(&issue_type));
        let summary = summary.unwrap_or_else(|| jira_issue_summary(&traceability));
        let description = description.unwrap_or_else(|| {
            jira_issue_description(&traceability, self.config.server.public_base_url.as_deref())
        });
        let labels = build_jira_labels(&traceability, labels);
        let work_item_label = work_item_label(&traceability.work_item);
        let dedupe_jql = format!(
            "project = \"{}\" AND labels = \"{}\" ORDER BY updated DESC",
            escape_jql_literal(&project_key),
            escape_jql_literal(&work_item_label)
        );
        let clients = self.atlassian_clients()?;
        let existing_links = traceability
            .links
            .iter()
            .filter(|link| link.system.eq_ignore_ascii_case("jira"))
            .cloned()
            .collect::<Vec<_>>();

        let mut upstream_action = String::new();
        let mut issue = if let Some(issue_key) = issue_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if update_existing {
                upstream_action = "updated".to_string();
                clients
                    .jira
                    .update_issue(
                        issue_key,
                        Some(&summary),
                        Some(&description),
                        Some(labels.as_slice()),
                        &fields,
                    )
                    .await?
            } else {
                upstream_action = "linked_existing".to_string();
                clients.jira.get_issue(issue_key).await?
            }
        } else {
            let mut issue = None;
            if let Some(existing_link) = existing_links
                .iter()
                .find(|link| link.reference_type.eq_ignore_ascii_case(&reference_type))
                .or_else(|| existing_links.first())
            {
                match if update_existing {
                    clients
                        .jira
                        .update_issue(
                            &existing_link.reference_key,
                            Some(&summary),
                            Some(&description),
                            Some(labels.as_slice()),
                            &fields,
                        )
                        .await
                } else {
                    clients.jira.get_issue(&existing_link.reference_key).await
                } {
                    Ok(found) => {
                        upstream_action = if update_existing {
                            "updated".to_string()
                        } else {
                            "linked_existing".to_string()
                        };
                        issue = Some(found);
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            issue_key = %existing_link.reference_key,
                            "stored jira traceability link could not be refreshed before dedupe; continuing"
                        );
                    }
                }
            }
            if issue.is_none() {
                if let Some(found) = clients
                    .jira
                    .search_issues(&dedupe_jql, 10)
                    .await?
                    .into_iter()
                    .next()
                {
                    if update_existing {
                        upstream_action = "deduped_updated".to_string();
                        issue = Some(
                            clients
                                .jira
                                .update_issue(
                                    &found.issue_key,
                                    Some(&summary),
                                    Some(&description),
                                    Some(labels.as_slice()),
                                    &fields,
                                )
                                .await?,
                        );
                    } else {
                        upstream_action = "deduped".to_string();
                        issue = Some(found);
                    }
                }
            }
            if let Some(issue) = issue {
                issue
            } else {
                upstream_action = "created".to_string();
                clients
                    .jira
                    .create_issue(
                        &project_key,
                        &summary,
                        &issue_type,
                        Some(&description),
                        labels.as_slice(),
                        &fields,
                    )
                    .await?
            }
        };

        if let Some(transition_name) = transition_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            clients
                .jira
                .transition_issue(&issue.issue_key, transition_name)
                .await?;
            issue = clients.jira.get_issue(&issue.issue_key).await?;
            upstream_action = format!("{}_transitioned", upstream_action);
        }

        let link = self
            .upsert_work_item_link(
                work_item_id,
                NewTraceabilityLink {
                    execution_id: resolved_execution_id,
                    finding_key: resolved_finding_key,
                    system: "jira".to_string(),
                    reference_type: reference_type.clone(),
                    reference_key: issue.issue_key.clone(),
                    title: Some(issue.summary.clone()),
                    status: Some(issue.status.clone()),
                    url: Some(issue.url.clone()),
                    metadata: link_metadata(
                        &json!({}),
                        json!({
                            "issue_id": issue.issue_id,
                            "project_key": issue.project_key,
                            "issue_type": issue.issue_type,
                            "labels": issue.labels,
                            "work_item_label": work_item_label,
                            "upstream_action": upstream_action,
                            "synced_at": now_utc().to_rfc3339(),
                        }),
                    ),
                },
            )
            .await?;

        let mut event = ConductorEvent::new(
            "atlassian.jira.linked",
            format!(
                "jira {} for work item {} -> {}",
                upstream_action, work_item_id, issue.issue_key
            ),
            json!({
                "work_item_id": work_item_id.to_string(),
                "execution_id": resolved_execution_id.map(|value| value.to_string()),
                "finding_key": link.finding_key.clone(),
                "reference_key": issue.issue_key,
                "reference_type": reference_type,
                "upstream_action": upstream_action,
            }),
        );
        event.work_item_id = Some(work_item_id);
        event.execution_id = resolved_execution_id;
        event.status = Some(link.status.clone().unwrap_or_else(|| "linked".to_string()));
        self.publish_event(event);

        Ok(ExternalLinkOperationResult {
            link,
            upstream_action,
            upstream: json!(issue),
        })
    }

    pub async fn link_work_item_confluence(
        &self,
        work_item_id: Uuid,
        request: ConfluencePageLinkRequest,
    ) -> Result<ExternalLinkOperationResult> {
        let traceability = self
            .work_item_traceability(work_item_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("work item {} not found", work_item_id))?;
        let ConfluencePageLinkRequest {
            execution_id,
            finding_key,
            page_id,
            space_key,
            title,
            body_storage,
            parent_page_id,
            labels,
            update_existing,
        } = request;
        let resolved_execution_id =
            execution_id.or_else(|| traceability.latest_execution.as_ref().map(|item| item.id));
        let resolved_finding_key = resolve_finding_key(finding_key, &traceability);
        let space_key = space_key
            .or_else(|| {
                self.config
                    .integrations
                    .atlassian
                    .confluence_space_key
                    .clone()
            })
            .ok_or_else(|| anyhow::anyhow!("atlassian confluence_space_key is not configured"))?;
        let title = title.unwrap_or_else(|| confluence_page_title(&traceability));
        let body_storage = body_storage.unwrap_or_else(|| {
            confluence_page_body(&traceability, self.config.server.public_base_url.as_deref())
        });
        let parent_page_id = parent_page_id.or_else(|| {
            self.config
                .integrations
                .atlassian
                .confluence_parent_page_id
                .clone()
        });
        let labels = build_confluence_labels(&traceability, labels);
        let work_item_label = work_item_label(&traceability.work_item);
        let clients = self.atlassian_clients()?;
        let existing_page_link = traceability
            .links
            .iter()
            .find(|link| link.system.eq_ignore_ascii_case("confluence"))
            .cloned();
        let (upstream_action, mut page) = if let Some(page_id) = page_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            (
                "updated".to_string(),
                clients
                    .confluence
                    .update_page(
                        page_id,
                        Some(&title),
                        &body_storage,
                        parent_page_id.as_deref(),
                    )
                    .await?,
            )
        } else if let Some(existing_link) = existing_page_link {
            match clients
                .confluence
                .update_page(
                    &existing_link.reference_key,
                    Some(&title),
                    &body_storage,
                    parent_page_id.as_deref(),
                )
                .await
            {
                Ok(page) => (
                    if update_existing {
                        "updated".to_string()
                    } else {
                        "refreshed".to_string()
                    },
                    page,
                ),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        page_id = %existing_link.reference_key,
                        "stored confluence traceability link could not be refreshed before title search; continuing"
                    );
                    if let Some(found) = clients
                        .confluence
                        .find_page_by_title(&space_key, &title)
                        .await?
                    {
                        (
                            "deduped_updated".to_string(),
                            clients
                                .confluence
                                .update_page(
                                    &found.page_id,
                                    Some(&title),
                                    &body_storage,
                                    parent_page_id.as_deref(),
                                )
                                .await?,
                        )
                    } else {
                        (
                            "created".to_string(),
                            clients
                                .confluence
                                .create_page(
                                    &space_key,
                                    &title,
                                    &body_storage,
                                    parent_page_id.as_deref(),
                                )
                                .await?,
                        )
                    }
                }
            }
        } else if let Some(found) = clients
            .confluence
            .find_page_by_title(&space_key, &title)
            .await?
        {
            (
                "deduped_updated".to_string(),
                clients
                    .confluence
                    .update_page(
                        &found.page_id,
                        Some(&title),
                        &body_storage,
                        parent_page_id.as_deref(),
                    )
                    .await?,
            )
        } else {
            (
                "created".to_string(),
                clients
                    .confluence
                    .create_page(&space_key, &title, &body_storage, parent_page_id.as_deref())
                    .await?,
            )
        };
        if !labels.is_empty() {
            clients
                .confluence
                .add_labels(&page.page_id, labels.as_slice())
                .await?;
            page = clients.confluence.get_page(&page.page_id).await?;
        }

        let link = self
            .upsert_work_item_link(
                work_item_id,
                NewTraceabilityLink {
                    execution_id: resolved_execution_id,
                    finding_key: resolved_finding_key,
                    system: "confluence".to_string(),
                    reference_type: "page".to_string(),
                    reference_key: page.page_id.clone(),
                    title: Some(page.title.clone()),
                    status: Some("current".to_string()),
                    url: Some(page.url.clone()),
                    metadata: link_metadata(
                        &json!({}),
                        json!({
                            "space_key": page.space_key,
                            "version_number": page.version_number,
                            "labels": page.labels,
                            "work_item_label": work_item_label,
                            "upstream_action": upstream_action,
                            "synced_at": now_utc().to_rfc3339(),
                        }),
                    ),
                },
            )
            .await?;

        let mut event = ConductorEvent::new(
            "atlassian.confluence.linked",
            format!(
                "confluence {} for work item {} -> {}",
                upstream_action, work_item_id, page.page_id
            ),
            json!({
                "work_item_id": work_item_id.to_string(),
                "execution_id": resolved_execution_id.map(|value| value.to_string()),
                "finding_key": link.finding_key.clone(),
                "reference_key": page.page_id,
                "reference_type": "page",
                "upstream_action": upstream_action,
            }),
        );
        event.work_item_id = Some(work_item_id);
        event.execution_id = resolved_execution_id;
        event.status = Some("current".to_string());
        self.publish_event(event);

        Ok(ExternalLinkOperationResult {
            link,
            upstream_action,
            upstream: json!(page),
        })
    }

    pub async fn sync_work_item_links(
        &self,
        work_item_id: Uuid,
        request: TraceabilitySyncRequest,
    ) -> Result<TraceabilitySyncResult> {
        self.repository
            .get_work_item(work_item_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("work item {} not found", work_item_id))?;
        self.sync_traceability_links(Some(work_item_id), request)
            .await
    }

    pub async fn sync_all_links(
        &self,
        request: TraceabilitySyncRequest,
    ) -> Result<TraceabilitySyncResult> {
        self.sync_traceability_links(None, request).await
    }

    pub async fn work_item_traceability(
        &self,
        work_item_id: Uuid,
    ) -> Result<Option<WorkItemTraceability>> {
        let Some(work_item) = self.repository.get_work_item(work_item_id).await? else {
            return Ok(None);
        };
        let services = self.repository.list_service_snapshots().await?;
        let repositories = self.repository.list_repository_snapshots().await?;
        let findings = self.repository.list_findings().await?;
        let mut executions = self
            .repository
            .list_work_executions_for_item(work_item_id, 100)
            .await?;
        executions.sort_by_key(execution_sort_key);
        executions.reverse();

        let finding = traceability_finding(&work_item, &findings);
        let evidence = if let Some(finding) = &finding {
            self.repository.list_finding_evidence(finding.id).await?
        } else {
            Vec::new()
        };
        let provenance = if let Some(finding) = &finding {
            self.repository.list_finding_provenance(finding.id).await?
        } else {
            Vec::new()
        };
        let links = self
            .repository
            .list_traceability_links(Some(work_item_id), None, None, 200)
            .await?;
        let target_service = traceability_service(&work_item, &services);
        let target_repository =
            traceability_repository(&finding, target_service.as_ref(), &repositories);
        let latest_execution = executions.first().cloned();
        let latest_verification = latest_execution
            .as_ref()
            .map(|execution| execution.verification.clone())
            .unwrap_or_else(|| json!({}));
        let independent_validation = latest_execution
            .as_ref()
            .and_then(traceability_independent_validation)
            .unwrap_or_else(|| json!({}));

        Ok(Some(WorkItemTraceability {
            work_item,
            finding,
            target_service,
            target_repository,
            evidence,
            provenance,
            links,
            executions,
            latest_execution,
            latest_verification,
            independent_validation,
        }))
    }

    pub async fn traceability_graph(&self) -> Result<TraceabilityGraph> {
        let services = self.repository.list_service_snapshots().await?;
        let repositories = self.repository.list_repository_snapshots().await?;
        let findings = self.repository.list_findings().await?;
        let work_items = self.repository.list_work_items().await?;
        let executions = self.repository.list_work_executions(5000).await?;
        let links = self
            .repository
            .list_traceability_links(None, None, None, 5000)
            .await?;
        Ok(build_traceability_graph(
            &services,
            &repositories,
            &findings,
            &work_items,
            &executions,
            &links,
        ))
    }

    fn atlassian_clients(&self) -> Result<AtlassianClients> {
        AtlassianClients::from_config(&self.config.integrations.atlassian)
    }

    async fn sync_traceability_links(
        &self,
        work_item_id: Option<Uuid>,
        request: TraceabilitySyncRequest,
    ) -> Result<TraceabilitySyncResult> {
        let limit = if work_item_id.is_some() { 200 } else { 5000 };
        let requested_systems = normalize_requested_systems(request.systems);
        let supported_systems = ["jira", "confluence", "refiner", "tracey"];
        let unsupported_systems = requested_systems
            .iter()
            .filter(|system| !supported_systems.contains(&system.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let mut synced_systems = Vec::new();
        let mut synced_links = Vec::new();
        let mut errors = Vec::new();

        let should_sync = |system: &str| {
            requested_systems.is_empty()
                || requested_systems
                    .iter()
                    .any(|requested| requested.eq_ignore_ascii_case(system))
        };

        if should_sync("refiner") {
            let (links, link_errors) = self.sync_refiner_traceability_links(work_item_id).await?;
            if !links.is_empty() {
                synced_systems.push("refiner".to_string());
                synced_links.extend(links);
            }
            errors.extend(link_errors);
        }

        if should_sync("tracey") {
            let (links, link_errors) = self.sync_tracey_traceability_links(work_item_id).await?;
            if !links.is_empty() {
                synced_systems.push("tracey".to_string());
                synced_links.extend(links);
            }
            errors.extend(link_errors);
        }

        if should_sync("jira") || should_sync("confluence") {
            let links = self
                .repository
                .list_traceability_links(work_item_id, None, None, limit)
                .await?;
            let links_to_sync = links
                .into_iter()
                .filter(|link| {
                    matches!(
                        link.system.to_ascii_lowercase().as_str(),
                        "jira" | "confluence"
                    ) && (requested_systems.is_empty()
                        || requested_systems
                            .iter()
                            .any(|system| system.eq_ignore_ascii_case(&link.system)))
                })
                .collect::<Vec<_>>();

            if !links_to_sync.is_empty() {
                let clients = self.atlassian_clients()?;
                for link in links_to_sync {
                    match self.refresh_traceability_link(&link, &clients).await {
                        Ok(updated) => {
                            synced_systems.push(updated.system.to_ascii_lowercase());
                            synced_links.push(updated);
                        }
                        Err(error) => {
                            errors
                                .push(format!("{} {}: {}", link.system, link.reference_key, error));
                            if let Err(store_error) =
                                self.record_link_sync_error(&link, &error.to_string()).await
                            {
                                tracing::warn!(
                                    error = %store_error,
                                    system = %link.system,
                                    reference_key = %link.reference_key,
                                    "failed to record link sync error"
                                );
                            }
                        }
                    }
                }
            }
        }

        let synced_systems = unique_strings(synced_systems);
        let synced_links = unique_traceability_links(synced_links);
        if !synced_links.is_empty() || !errors.is_empty() {
            let mut event = ConductorEvent::new(
                "traceability.sync.completed",
                if errors.is_empty() {
                    format!("synced {} external link(s)", synced_links.len())
                } else {
                    format!(
                        "synced {} external link(s) with {} error(s)",
                        synced_links.len(),
                        errors.len()
                    )
                },
                json!({
                    "work_item_id": work_item_id.map(|value| value.to_string()),
                    "requested_systems": requested_systems,
                    "synced_systems": synced_systems,
                    "unsupported_systems": unsupported_systems,
                    "links_synced": synced_links.len(),
                    "errors": errors,
                }),
            );
            event.work_item_id = work_item_id;
            event.status = Some(if errors.is_empty() {
                "success".to_string()
            } else {
                "partial_failure".to_string()
            });
            self.publish_event(event);
        }

        Ok(TraceabilitySyncResult {
            work_item_id,
            requested_systems,
            synced_systems,
            unsupported_systems,
            errors,
            links: synced_links,
        })
    }

    async fn sync_refiner_traceability_links(
        &self,
        work_item_id: Option<Uuid>,
    ) -> Result<(Vec<TraceabilityLink>, Vec<String>)> {
        let executions = if let Some(work_item_id) = work_item_id {
            self.repository
                .list_work_executions_for_item(work_item_id, 100)
                .await?
        } else {
            self.repository.list_work_executions(1000).await?
        };
        let executions = executions
            .into_iter()
            .filter(|execution| execution.refiner_job_id.is_some())
            .collect::<Vec<_>>();
        if executions.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let work_items = self.repository.list_work_items().await?;
        let work_items_by_id = work_items
            .into_iter()
            .map(|item| (item.id, item))
            .collect::<HashMap<_, _>>();
        let services = self.repository.list_service_snapshots().await?;
        let refiner_service = services
            .iter()
            .find(|service| service.service_key == "refiner");
        let mut errors = Vec::new();
        let mut refiner_client =
            match RefinerClient::from_sources(&self.config.integrations.refiner, refiner_service) {
                Ok(client) => client,
                Err(error) => {
                    errors.push(format!("refiner client unavailable: {}", error));
                    None
                }
            };

        if let Some(client) = refiner_client.as_ref() {
            if let Err(error) = client.login_if_configured().await {
                errors.push(format!("refiner login unavailable: {}", error));
                refiner_client = None;
            }
        }

        let mut synced = Vec::new();
        for execution in executions {
            let Some(work_item) = work_items_by_id.get(&execution.work_item_id) else {
                continue;
            };
            let Some(job_id) = execution.refiner_job_id.as_deref() else {
                continue;
            };

            let mut job_detail = None;
            let mut requirements_progress = None;
            let mut requirements_summary = None;
            let mut workspace = None;
            let mut sync_error = None;

            if let Some(client) = refiner_client.as_ref() {
                match client.get_job(job_id).await {
                    Ok(detail) => {
                        job_detail = Some(detail);
                        match client.get_requirements_progress(job_id).await {
                            Ok(progress) => requirements_progress = Some(progress),
                            Err(error) => errors.push(format!(
                                "refiner job {} requirements progress: {}",
                                job_id, error
                            )),
                        }
                        match client.get_requirements_summary(job_id).await {
                            Ok(summary) => requirements_summary = Some(summary),
                            Err(error) => errors.push(format!(
                                "refiner job {} requirements summary: {}",
                                job_id, error
                            )),
                        }
                        match client.get_workspace(job_id).await {
                            Ok(payload) => workspace = Some(payload),
                            Err(error) => {
                                errors.push(format!("refiner job {} workspace: {}", job_id, error))
                            }
                        }
                    }
                    Err(error) => {
                        let message = error.to_string();
                        errors.push(format!("refiner job {}: {}", job_id, message));
                        sync_error = Some(message);
                    }
                }
            }

            for request in build_refiner_traceability_requests(
                refiner_client.as_ref(),
                work_item,
                &execution,
                job_detail.as_ref(),
                requirements_progress.as_ref(),
                requirements_summary.as_ref(),
                workspace.as_ref(),
                sync_error.as_deref(),
            ) {
                let link = TraceabilityLink::from_new(Some(work_item.id), request);
                self.repository.upsert_traceability_link(&link).await?;
                synced.push(link);
            }
        }

        Ok((unique_traceability_links(synced), unique_strings(errors)))
    }

    async fn sync_tracey_traceability_links(
        &self,
        work_item_id: Option<Uuid>,
    ) -> Result<(Vec<TraceabilityLink>, Vec<String>)> {
        let work_items = self.repository.list_work_items().await?;
        let work_items_by_id = work_items
            .iter()
            .cloned()
            .map(|item| (item.id, item))
            .collect::<HashMap<_, _>>();
        let executions = if let Some(work_item_id) = work_item_id {
            self.repository
                .list_work_executions_for_item(work_item_id, 100)
                .await?
        } else {
            self.repository.list_work_executions(1000).await?
        };

        let candidate = if let Some(work_item_id) = work_item_id {
            let work_item = work_items_by_id.get(&work_item_id).cloned();
            let execution = executions
                .into_iter()
                .max_by_key(execution_sort_key)
                .filter(|execution| targets_service("tracey", Some(execution), work_item.as_ref()));
            match (work_item, execution) {
                (Some(work_item), Some(execution)) => Some((work_item, execution)),
                _ => None,
            }
        } else {
            executions
                .into_iter()
                .filter_map(|execution| {
                    let work_item = work_items_by_id.get(&execution.work_item_id)?.clone();
                    if targets_service("tracey", Some(&execution), Some(&work_item)) {
                        Some((work_item, execution))
                    } else {
                        None
                    }
                })
                .max_by_key(|(_, execution)| execution_sort_key(execution))
        };

        let Some((work_item, execution)) = candidate else {
            return Ok((Vec::new(), Vec::new()));
        };

        let services = self.repository.list_service_snapshots().await?;
        let tracey_service = services
            .iter()
            .find(|service| service.service_key == "tracey");
        let mut errors = Vec::new();
        let mut status = tracey_service
            .and_then(tracey_status_from_probe)
            .or_else(|| tracey_service.and_then(tracey_status_from_probe_root));
        let mut loader_status = tracey_service.and_then(tracey_loader_status_from_probe);

        if let Some(client) =
            match TraceyClient::from_sources(&self.config.integrations.tracey, tracey_service) {
                Ok(client) => client,
                Err(error) => {
                    errors.push(format!("tracey client unavailable: {}", error));
                    None
                }
            }
        {
            match client.status().await {
                Ok(live_status) => status = Some(live_status),
                Err(error) => errors.push(format!("tracey status: {}", error)),
            }
            match client.loader_status().await {
                Ok(live_loader_status) => {
                    if live_loader_status.is_some() {
                        loader_status = live_loader_status;
                    }
                }
                Err(error) => errors.push(format!("tracey loader status: {}", error)),
            }

            let synced = build_tracey_traceability_requests(
                Some(&client),
                &work_item,
                &execution,
                status.as_ref(),
                loader_status.as_ref(),
                tracey_service,
            );
            let mut stored = Vec::new();
            for request in synced {
                let link = TraceabilityLink::from_new(Some(work_item.id), request);
                self.repository.upsert_traceability_link(&link).await?;
                stored.push(link);
            }
            return Ok((unique_traceability_links(stored), unique_strings(errors)));
        }

        let synced = build_tracey_traceability_requests(
            None,
            &work_item,
            &execution,
            status.as_ref(),
            loader_status.as_ref(),
            tracey_service,
        );
        let mut stored = Vec::new();
        for request in synced {
            let link = TraceabilityLink::from_new(Some(work_item.id), request);
            self.repository.upsert_traceability_link(&link).await?;
            stored.push(link);
        }

        Ok((unique_traceability_links(stored), unique_strings(errors)))
    }

    async fn refresh_traceability_link(
        &self,
        link: &TraceabilityLink,
        clients: &AtlassianClients,
    ) -> Result<TraceabilityLink> {
        match link.system.trim().to_ascii_lowercase().as_str() {
            "jira" => {
                let issue = clients.jira.get_issue(&link.reference_key).await?;
                let metadata = link_metadata(
                    &link.metadata,
                    json!({
                        "issue_id": issue.issue_id,
                        "project_key": issue.project_key,
                        "issue_type": issue.issue_type,
                        "labels": issue.labels,
                        "upstream_action": "synced",
                        "synced_at": now_utc().to_rfc3339(),
                    }),
                );
                self.replace_traceability_link(
                    link,
                    NewTraceabilityLink {
                        execution_id: link.execution_id,
                        finding_key: link.finding_key.clone(),
                        system: link.system.clone(),
                        reference_type: link.reference_type.clone(),
                        reference_key: issue.issue_key.clone(),
                        title: Some(issue.summary.clone()),
                        status: Some(issue.status.clone()),
                        url: Some(issue.url.clone()),
                        metadata,
                    },
                )
                .await
            }
            "confluence" => {
                let page = clients.confluence.get_page(&link.reference_key).await?;
                let metadata = link_metadata(
                    &link.metadata,
                    json!({
                        "space_key": page.space_key,
                        "version_number": page.version_number,
                        "labels": page.labels,
                        "upstream_action": "synced",
                        "synced_at": now_utc().to_rfc3339(),
                    }),
                );
                self.replace_traceability_link(
                    link,
                    NewTraceabilityLink {
                        execution_id: link.execution_id,
                        finding_key: link.finding_key.clone(),
                        system: link.system.clone(),
                        reference_type: link.reference_type.clone(),
                        reference_key: page.page_id.clone(),
                        title: Some(page.title.clone()),
                        status: Some("current".to_string()),
                        url: Some(page.url.clone()),
                        metadata,
                    },
                )
                .await
            }
            system => Err(anyhow::anyhow!("unsupported sync system {}", system)),
        }
    }

    async fn replace_traceability_link(
        &self,
        existing: &TraceabilityLink,
        input: NewTraceabilityLink,
    ) -> Result<TraceabilityLink> {
        let mut updated = existing.clone();
        updated.apply_update(input);
        self.repository.upsert_traceability_link(&updated).await?;
        Ok(updated)
    }

    async fn record_link_sync_error(
        &self,
        link: &TraceabilityLink,
        error: &str,
    ) -> Result<TraceabilityLink> {
        self.replace_traceability_link(
            link,
            NewTraceabilityLink {
                execution_id: link.execution_id,
                finding_key: link.finding_key.clone(),
                system: link.system.clone(),
                reference_type: link.reference_type.clone(),
                reference_key: link.reference_key.clone(),
                title: link.title.clone(),
                status: link.status.clone(),
                url: link.url.clone(),
                metadata: sync_error_metadata(&link.metadata, error),
            },
        )
        .await
    }

    pub async fn run_execution_cycle(&self) -> Result<Vec<WorkExecution>> {
        self.publish_event(ConductorEvent::new(
            "execution.cycle.started",
            "execution cycle started",
            json!({}),
        ));
        let callback = self.event_callback();
        let result =
            run_execution_cycle(self.repository.as_ref(), &self.config, Some(&callback)).await;
        match &result {
            Ok(executions) => {
                let mut event = ConductorEvent::new(
                    "execution.cycle.completed",
                    format!(
                        "execution cycle completed with {} execution(s)",
                        executions.len()
                    ),
                    json!({
                        "executions_started": executions.len(),
                    }),
                );
                event.status = Some("success".to_string());
                self.publish_event(event);
            }
            Err(error) => {
                let mut event = ConductorEvent::new(
                    "execution.cycle.failed",
                    format!("execution cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                self.publish_event(event);
            }
        }
        result
    }

    pub async fn execute_work_item(
        &self,
        work_item_id: Uuid,
        force_schedule: bool,
    ) -> Result<WorkExecution> {
        self.publish_event(ConductorEvent::new(
            "execution.manual.started",
            format!("manual execution requested for work item {}", work_item_id),
            json!({
                "work_item_id": work_item_id.to_string(),
                "force_schedule": force_schedule,
            }),
        ));
        let callback = self.event_callback();
        execute_specific_work_item(
            self.repository.as_ref(),
            &self.config,
            work_item_id,
            force_schedule,
            Some(&callback),
        )
        .await
    }

    pub async fn summary(&self) -> Result<DashboardSummary> {
        let services = self.repository.list_service_snapshots().await?;
        let repositories = self.repository.list_repository_snapshots().await?;
        let findings = self.repository.list_findings().await?;
        let work_items = self.repository.list_work_items().await?;
        let latest_discovery = self
            .repository
            .list_discovery_runs(1)
            .await?
            .into_iter()
            .next();
        let latest_cycle = self
            .repository
            .list_improvement_cycles(1)
            .await?
            .into_iter()
            .next();
        let cycles_total = self.repository.list_improvement_cycles(200).await?.len();
        let executions = self.repository.list_work_executions(1000).await?;
        let links = self
            .repository
            .list_traceability_links(None, None, None, 5000)
            .await?;

        let mut work_by_status = BTreeMap::new();
        for item in &work_items {
            *work_by_status
                .entry(item.status.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let mut delivery_stage_totals = BTreeMap::new();
        let mut rollout_strategy_totals = BTreeMap::new();
        for item in &work_items {
            *delivery_stage_totals
                .entry(item.delivery_stage.as_str().to_string())
                .or_insert(0usize) += 1;
            *rollout_strategy_totals
                .entry(item.rollout_strategy.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let mut findings_by_severity = BTreeMap::new();
        for finding in &findings {
            *findings_by_severity
                .entry(finding.severity.as_str().to_string())
                .or_insert(0usize) += 1;
        }

        let mut external_reference_totals = BTreeMap::new();
        for link in &links {
            *external_reference_totals
                .entry(link.bucket())
                .or_insert(0usize) += 1;
        }

        let dora_metrics = compute_dora_metrics(
            &work_items,
            &executions,
            &links,
            self.config.delivery.dora_window_days,
        );

        Ok(DashboardSummary {
            generated_at: now_utc(),
            services_total: services.len(),
            repositories_total: repositories.len(),
            findings_total: findings.len(),
            findings_by_severity,
            services_healthy: services
                .iter()
                .filter(|service| matches!(service.health, crate::models::ServiceHealth::Healthy))
                .count(),
            services_degraded: services
                .iter()
                .filter(|service| matches!(service.health, crate::models::ServiceHealth::Degraded))
                .count(),
            services_unreachable: services
                .iter()
                .filter(|service| {
                    matches!(
                        service.health,
                        crate::models::ServiceHealth::Unreachable
                            | crate::models::ServiceHealth::Missing
                    )
                })
                .count(),
            work_items_total: work_items.len(),
            work_by_status,
            delivery_stage_totals,
            rollout_strategy_totals,
            external_reference_totals,
            cycles_total,
            executions_total: executions.len(),
            executions_running: executions
                .iter()
                .filter(|execution| !execution.status.is_terminal())
                .count(),
            approvals_waiting: work_items
                .iter()
                .filter(|item| {
                    !item.execution_approved
                        && matches!(
                            item.status,
                            WorkStatus::Planned | WorkStatus::Scheduled | WorkStatus::OnHold
                        )
                })
                .count(),
            dora_metrics,
            latest_discovery,
            latest_cycle,
        })
    }

    pub async fn topology(&self) -> Result<crate::models::TopologyGraph> {
        let services = self.repository.list_service_snapshots().await?;
        Ok(topology_from_services(&services))
    }
}

fn compute_dora_metrics(
    work_items: &[WorkItem],
    executions: &[WorkExecution],
    links: &[TraceabilityLink],
    window_days: i64,
) -> DoraMetricsSummary {
    let window_days = window_days.max(1);
    let cutoff = now_utc() - ChronoDuration::days(window_days);
    let work_items_by_id = work_items
        .iter()
        .map(|item| (item.id, item))
        .collect::<HashMap<_, _>>();
    let mut links_by_work_item: HashMap<Uuid, Vec<&TraceabilityLink>> = HashMap::new();
    let mut links_by_execution: HashMap<Uuid, Vec<&TraceabilityLink>> = HashMap::new();
    for link in links {
        if let Some(work_item_id) = link.work_item_id {
            links_by_work_item
                .entry(work_item_id)
                .or_default()
                .push(link);
        }
        if let Some(execution_id) = link.execution_id {
            links_by_execution
                .entry(execution_id)
                .or_default()
                .push(link);
        }
    }

    let mut production_executions = executions
        .iter()
        .filter(|execution| matches!(execution.delivery_stage, DeliveryStage::Production))
        .filter(|execution| execution.finished_at.unwrap_or(execution.updated_at) >= cutoff)
        .collect::<Vec<_>>();
    production_executions
        .sort_by_key(|execution| execution.finished_at.unwrap_or(execution.updated_at));

    let attempted_production_deployments = production_executions.len();
    let incident_linked_production_deployments = production_executions
        .iter()
        .filter(|execution| {
            execution_links(execution, &links_by_execution, &links_by_work_item)
                .iter()
                .any(|link| link.is_incident_signal())
        })
        .count();
    let bug_linked_production_deployments = production_executions
        .iter()
        .filter(|execution| {
            execution_links(execution, &links_by_execution, &links_by_work_item)
                .iter()
                .any(|link| link.is_bug_signal())
        })
        .count();
    let successful_executions = production_executions
        .iter()
        .copied()
        .filter(|execution| matches!(execution.status, crate::models::ExecutionStatus::Success))
        .collect::<Vec<_>>();
    let failed_executions = production_executions
        .iter()
        .copied()
        .filter(|execution| {
            matches!(
                execution.status,
                crate::models::ExecutionStatus::Failure
                    | crate::models::ExecutionStatus::Blocked
                    | crate::models::ExecutionStatus::Cancelled
            )
        })
        .collect::<Vec<_>>();
    let correlated_failure_deployments = production_executions
        .iter()
        .filter(|execution| {
            matches!(
                execution.status,
                crate::models::ExecutionStatus::Failure
                    | crate::models::ExecutionStatus::Blocked
                    | crate::models::ExecutionStatus::Cancelled
            ) || execution_links(execution, &links_by_execution, &links_by_work_item)
                .iter()
                .any(|link| link.is_bug_signal() || link.is_incident_signal())
        })
        .count();

    let mut lead_times = successful_executions
        .iter()
        .filter_map(|execution| {
            let item = work_items_by_id.get(&execution.work_item_id)?;
            let finished_at = execution.finished_at?;
            Some((finished_at - item.created_at).num_minutes() as f64 / 60.0)
        })
        .collect::<Vec<_>>();
    lead_times.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));

    let mut mttr_values = Vec::new();
    for failed in &failed_executions {
        let failed_at = failed.finished_at.unwrap_or(failed.updated_at);
        if let Some(recovery) = successful_executions.iter().find(|candidate| {
            let candidate_finished = candidate.finished_at.unwrap_or(candidate.updated_at);
            candidate_finished > failed_at && same_recovery_scope(candidate, failed)
        }) {
            let recovered_at = recovery.finished_at.unwrap_or(recovery.updated_at);
            mttr_values.push((recovered_at - failed_at).num_minutes() as f64 / 60.0);
        }
    }

    DoraMetricsSummary {
        window_days,
        attempted_production_deployments,
        successful_production_deployments: successful_executions.len(),
        deployment_frequency_per_day: successful_executions.len() as f64 / window_days as f64,
        lead_time_hours_average: average(&lead_times),
        lead_time_hours_median: median(&lead_times),
        change_failure_rate_pct: if attempted_production_deployments == 0 {
            0.0
        } else {
            (failed_executions.len() as f64 / attempted_production_deployments as f64) * 100.0
        },
        correlated_change_failure_rate_pct: if attempted_production_deployments == 0 {
            0.0
        } else {
            (correlated_failure_deployments as f64 / attempted_production_deployments as f64)
                * 100.0
        },
        incident_linked_production_deployments,
        bug_linked_production_deployments,
        mean_time_to_restore_hours: average(&mttr_values),
    }
}

fn execution_links<'a>(
    execution: &WorkExecution,
    links_by_execution: &'a HashMap<Uuid, Vec<&'a TraceabilityLink>>,
    links_by_work_item: &'a HashMap<Uuid, Vec<&'a TraceabilityLink>>,
) -> Vec<&'a TraceabilityLink> {
    let mut links = Vec::new();
    if let Some(items) = links_by_execution.get(&execution.id) {
        links.extend(items.iter().copied());
    }
    if let Some(items) = links_by_work_item.get(&execution.work_item_id) {
        for link in items {
            if !links
                .iter()
                .any(|candidate| candidate.link_key == link.link_key)
            {
                links.push(*link);
            }
        }
    }
    links
}

fn build_traceability_graph(
    services: &[ServiceSnapshot],
    repositories: &[RepositorySnapshot],
    findings: &[FindingRecord],
    work_items: &[WorkItem],
    executions: &[WorkExecution],
    links: &[TraceabilityLink],
) -> TraceabilityGraph {
    let mut nodes = BTreeMap::new();
    let mut edges = BTreeMap::new();

    for service in services {
        let service_id = traceability_graph_node_id("service", &service.service_key);
        upsert_graph_node(
            &mut nodes,
            TraceabilityGraphNode {
                id: service_id.clone(),
                kind: "service".to_string(),
                label: service.display_name.clone(),
                summary: Some(format!("{} · {}", service.kind, service.health.as_str())),
                status: Some(service.health.as_str().to_string()),
                metadata: json!({
                    "service_key": service.service_key,
                    "role_name": service.role_name.clone(),
                    "deployment_environment": service.deployment_environment.map(|value| value.as_str()),
                    "dependencies": service.dependencies.clone(),
                    "capabilities": service.capabilities.clone(),
                    "internal_url": service.internal_url.clone(),
                    "public_url": service.public_url.clone(),
                    "repo_path": service.repo_path.clone(),
                    "repo_url": service.repo_url.clone(),
                }),
            },
        );

        for dependency in &service.dependencies {
            let dependency_id = ensure_service_graph_node(&mut nodes, dependency);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: service_id.clone(),
                    to: dependency_id,
                    relationship: "depends_on_service".to_string(),
                    metadata: json!({"source": "service_snapshot"}),
                },
            );
        }
    }

    for repository in repositories {
        let repository_id = traceability_graph_node_id("repository", &repository.repo_key);
        upsert_graph_node(
            &mut nodes,
            TraceabilityGraphNode {
                id: repository_id.clone(),
                kind: "repository".to_string(),
                label: repository.name.clone(),
                summary: Some(
                    repository
                        .purpose
                        .clone()
                        .or_else(|| repository.runtime_type.clone())
                        .unwrap_or_else(|| "repository inventory".to_string()),
                ),
                status: Some(if repository.archived {
                    "archived".to_string()
                } else {
                    "active".to_string()
                }),
                metadata: json!({
                    "repo_key": repository.repo_key,
                    "owner": repository.owner.clone(),
                    "repo_url": repository.repo_url.clone(),
                    "local_path": repository.local_path.clone(),
                    "language": repository.language.clone(),
                    "frameworks": repository.frameworks.clone(),
                    "build_systems": repository.build_systems.clone(),
                    "package_managers": repository.package_managers.clone(),
                    "runtime_type": repository.runtime_type.clone(),
                    "deployment_type": repository.deployment_type.clone(),
                    "criticality": repository.criticality.clone(),
                    "visibility": repository.visibility.clone(),
                    "default_branch": repository.default_branch.clone(),
                    "current_branch": repository.current_branch.clone(),
                }),
            },
        );

        for service_key in &repository.linked_services {
            let service_id = ensure_service_graph_node(&mut nodes, service_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: service_id,
                    to: repository_id.clone(),
                    relationship: "implemented_by_repository".to_string(),
                    metadata: json!({"source": "repository_snapshot"}),
                },
            );
        }

        for dependency in &repository.dependencies {
            let dependency_id = ensure_repository_graph_node(&mut nodes, dependency);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: repository_id.clone(),
                    to: dependency_id,
                    relationship: "depends_on_repository".to_string(),
                    metadata: json!({"source": "repository_snapshot"}),
                },
            );
        }
    }

    for finding in findings {
        let finding_id = traceability_graph_node_id("finding", &finding.finding_key);
        upsert_graph_node(
            &mut nodes,
            TraceabilityGraphNode {
                id: finding_id.clone(),
                kind: "finding".to_string(),
                label: finding.title.clone(),
                summary: Some(finding.summary.clone()),
                status: Some(finding.status.as_str().to_string()),
                metadata: json!({
                    "finding_id": finding.id,
                    "finding_key": finding.finding_key,
                    "severity": finding.severity.as_str(),
                    "category": finding.category.clone(),
                    "target_service": finding.target_service.clone(),
                    "target_repository": finding.target_repository.clone(),
                    "confidence_score": finding.confidence_score,
                    "tags": finding.tags.clone(),
                    "last_seen_at": finding.last_seen_at,
                }),
            },
        );

        if let Some(service_key) = finding.target_service.as_deref() {
            let service_id = ensure_service_graph_node(&mut nodes, service_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: finding_id.clone(),
                    to: service_id,
                    relationship: "affects_service".to_string(),
                    metadata: json!({"source": "finding"}),
                },
            );
        }

        if let Some(repository_key) = finding.target_repository.as_deref() {
            let repository_id = ensure_repository_graph_node(&mut nodes, repository_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: finding_id,
                    to: repository_id,
                    relationship: "affects_repository".to_string(),
                    metadata: json!({"source": "finding"}),
                },
            );
        }
    }

    for work_item in work_items {
        let work_item_id = traceability_graph_node_id("work_item", &work_item.id.to_string());
        upsert_graph_node(
            &mut nodes,
            TraceabilityGraphNode {
                id: work_item_id.clone(),
                kind: "work_item".to_string(),
                label: work_item.title.clone(),
                summary: Some(work_item.summary.clone()),
                status: Some(work_item.status.as_str().to_string()),
                metadata: json!({
                    "work_item_id": work_item.id,
                    "delivery_stage": work_item.delivery_stage.as_str(),
                    "rollout_strategy": work_item.rollout_strategy.as_str(),
                    "validated_stages": work_item.validated_stages.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                    "priority": work_item.priority,
                    "progress_pct": work_item.progress_pct,
                    "target_service": work_item.target_service.clone(),
                    "source": work_item.source.clone(),
                    "scheduled_for": work_item.scheduled_for.clone(),
                    "depends_on": work_item.depends_on.clone(),
                    "tags": work_item.tags.clone(),
                }),
            },
        );

        if let Some(finding) = traceability_finding(work_item, findings) {
            let finding_id = traceability_graph_node_id("finding", &finding.finding_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: work_item_id.clone(),
                    to: finding_id,
                    relationship: "addresses_finding".to_string(),
                    metadata: json!({"source": "work_item.plan"}),
                },
            );
        }

        if let Some(service_key) = work_item.target_service.as_deref() {
            let service_id = ensure_service_graph_node(&mut nodes, service_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: work_item_id.clone(),
                    to: service_id.clone(),
                    relationship: "targets_service".to_string(),
                    metadata: json!({"source": "work_item"}),
                },
            );

            if let Some(service) = services
                .iter()
                .find(|service| service.service_key == service_key)
            {
                if let Some(repository) = traceability_repository(
                    &traceability_finding(work_item, findings),
                    Some(service),
                    repositories,
                ) {
                    let repository_id =
                        traceability_graph_node_id("repository", &repository.repo_key);
                    upsert_graph_edge(
                        &mut edges,
                        TraceabilityGraphEdge {
                            from: work_item_id.clone(),
                            to: repository_id,
                            relationship: "targets_repository".to_string(),
                            metadata: json!({"source": "work_item"}),
                        },
                    );
                }
            }
        }

        for dependency in &work_item.depends_on {
            if let Some(target) = work_items
                .iter()
                .find(|item| item.matches_reference(dependency))
            {
                let dependency_id = traceability_graph_node_id("work_item", &target.id.to_string());
                upsert_graph_edge(
                    &mut edges,
                    TraceabilityGraphEdge {
                        from: work_item_id.clone(),
                        to: dependency_id,
                        relationship: "depends_on_work_item".to_string(),
                        metadata: json!({"source": "work_item.depends_on", "reference": dependency}),
                    },
                );
            }
        }
    }

    for execution in executions {
        let execution_id = traceability_graph_node_id("execution", &execution.id.to_string());
        upsert_graph_node(
            &mut nodes,
            TraceabilityGraphNode {
                id: execution_id.clone(),
                kind: "execution".to_string(),
                label: format!(
                    "{} / {}",
                    execution.delivery_stage.as_str(),
                    execution.rollout_strategy.as_str()
                ),
                summary: execution
                    .refiner_job_id
                    .as_ref()
                    .map(|job_id| format!("Refiner job {}", job_id))
                    .or_else(|| Some("Governed execution".to_string())),
                status: Some(execution.status.as_str().to_string()),
                metadata: json!({
                    "execution_id": execution.id,
                    "work_item_id": execution.work_item_id,
                    "delivery_stage": execution.delivery_stage.as_str(),
                    "rollout_strategy": execution.rollout_strategy.as_str(),
                    "target_service": execution.target_service.clone(),
                    "refiner_job_id": execution.refiner_job_id.clone(),
                    "started_at": execution.started_at,
                    "updated_at": execution.updated_at,
                    "finished_at": execution.finished_at,
                }),
            },
        );

        let work_item_id =
            traceability_graph_node_id("work_item", &execution.work_item_id.to_string());
        upsert_graph_edge(
            &mut edges,
            TraceabilityGraphEdge {
                from: work_item_id,
                to: execution_id.clone(),
                relationship: "has_execution".to_string(),
                metadata: json!({"source": "work_execution"}),
            },
        );

        if let Some(service_key) = execution.target_service.as_deref() {
            let service_id = ensure_service_graph_node(&mut nodes, service_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: execution_id,
                    to: service_id,
                    relationship: "executes_against_service".to_string(),
                    metadata: json!({"source": "work_execution"}),
                },
            );
        }
    }

    for link in links {
        let link_id = traceability_graph_node_id("link", &link.link_key);
        upsert_graph_node(
            &mut nodes,
            TraceabilityGraphNode {
                id: link_id.clone(),
                kind: "external_link".to_string(),
                label: format!(
                    "{} {} {}",
                    link.system.as_str(),
                    link.reference_type.as_str(),
                    link.reference_key.as_str()
                ),
                summary: link.title.clone(),
                status: link.status.clone(),
                metadata: json!({
                    "link_id": link.id,
                    "link_key": link.link_key,
                    "work_item_id": link.work_item_id,
                    "execution_id": link.execution_id,
                    "finding_key": link.finding_key.clone(),
                    "system": link.system.clone(),
                    "reference_type": link.reference_type.clone(),
                    "reference_key": link.reference_key.clone(),
                    "url": link.url.clone(),
                    "detail": link.metadata.clone(),
                    "updated_at": link.updated_at,
                }),
            },
        );

        if let Some(work_item_id) = link.work_item_id {
            let work_item_node_id =
                traceability_graph_node_id("work_item", &work_item_id.to_string());
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: work_item_node_id,
                    to: link_id.clone(),
                    relationship: "references_external".to_string(),
                    metadata: json!({"source": "traceability_link"}),
                },
            );
        }

        if let Some(execution_id) = link.execution_id {
            let execution_node_id =
                traceability_graph_node_id("execution", &execution_id.to_string());
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: execution_node_id,
                    to: link_id.clone(),
                    relationship: "emits_traceability".to_string(),
                    metadata: json!({"source": "traceability_link"}),
                },
            );
        }

        if let Some(finding_key) = link.finding_key.as_deref() {
            let finding_id = ensure_finding_graph_node(&mut nodes, finding_key);
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: link_id.clone(),
                    to: finding_id,
                    relationship: "tracks_finding".to_string(),
                    metadata: json!({"source": "traceability_link"}),
                },
            );
        }

        if services
            .iter()
            .any(|service| service.service_key == link.system.as_str())
        {
            let service_id = traceability_graph_node_id("service", link.system.as_str());
            upsert_graph_edge(
                &mut edges,
                TraceabilityGraphEdge {
                    from: service_id,
                    to: link_id,
                    relationship: "reported_by_service".to_string(),
                    metadata: json!({"source": "traceability_link.system"}),
                },
            );
        }
    }

    let mut node_totals = BTreeMap::new();
    for node in nodes.values() {
        *node_totals.entry(node.kind.clone()).or_insert(0usize) += 1;
    }

    let mut relationship_totals = BTreeMap::new();
    for edge in edges.values() {
        *relationship_totals
            .entry(edge.relationship.clone())
            .or_insert(0usize) += 1;
    }

    TraceabilityGraph {
        generated_at: now_utc(),
        node_totals,
        relationship_totals,
        nodes: nodes.into_values().collect(),
        edges: edges.into_values().collect(),
    }
}

fn traceability_graph_node_id(kind: &str, key: &str) -> String {
    format!("{}:{}", kind.trim(), key.trim())
}

fn persist_conductor_event_async(repository: Arc<dyn ConductorRepository>, event: ConductorEvent) {
    tokio::spawn(async move {
        let mut delay_ms = 25u64;
        for attempt in 0..6 {
            match repository.insert_conductor_event(&event).await {
                Ok(()) => return,
                Err(error) => {
                    if attempt < 5 && event_persistence_should_retry(&error) {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = (delay_ms.saturating_mul(2)).min(1000);
                        continue;
                    }
                    tracing::warn!(
                        error = %error,
                        attempts = attempt + 1,
                        event_type = %event.event_type,
                        work_item_id = ?event.work_item_id,
                        execution_id = ?event.execution_id,
                        "failed to persist conductor event"
                    );
                    return;
                }
            }
        }
    });
}

fn event_persistence_should_retry(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("foreign key constraint")
        || message.contains("violates foreign key constraint")
}

fn upsert_graph_node(
    nodes: &mut BTreeMap<String, TraceabilityGraphNode>,
    node: TraceabilityGraphNode,
) {
    nodes.entry(node.id.clone()).or_insert(node);
}

fn upsert_graph_edge(
    edges: &mut BTreeMap<String, TraceabilityGraphEdge>,
    edge: TraceabilityGraphEdge,
) {
    let key = format!("{}|{}|{}", edge.from, edge.relationship, edge.to);
    edges.entry(key).or_insert(edge);
}

fn ensure_service_graph_node(
    nodes: &mut BTreeMap<String, TraceabilityGraphNode>,
    service_key: &str,
) -> String {
    let node_id = traceability_graph_node_id("service", service_key);
    upsert_graph_node(
        nodes,
        TraceabilityGraphNode {
            id: node_id.clone(),
            kind: "service".to_string(),
            label: service_key.to_string(),
            summary: Some("Referenced service dependency".to_string()),
            status: Some("unknown".to_string()),
            metadata: json!({"service_key": service_key, "placeholder": true}),
        },
    );
    node_id
}

fn ensure_repository_graph_node(
    nodes: &mut BTreeMap<String, TraceabilityGraphNode>,
    repository_key: &str,
) -> String {
    let node_id = traceability_graph_node_id("repository", repository_key);
    upsert_graph_node(
        nodes,
        TraceabilityGraphNode {
            id: node_id.clone(),
            kind: "repository".to_string(),
            label: repository_key.to_string(),
            summary: Some("Referenced repository dependency".to_string()),
            status: Some("unknown".to_string()),
            metadata: json!({"repo_key": repository_key, "placeholder": true}),
        },
    );
    node_id
}

fn ensure_finding_graph_node(
    nodes: &mut BTreeMap<String, TraceabilityGraphNode>,
    finding_key: &str,
) -> String {
    let node_id = traceability_graph_node_id("finding", finding_key);
    upsert_graph_node(
        nodes,
        TraceabilityGraphNode {
            id: node_id.clone(),
            kind: "finding".to_string(),
            label: finding_key.to_string(),
            summary: Some("Referenced finding link".to_string()),
            status: Some("unknown".to_string()),
            metadata: json!({"finding_key": finding_key, "placeholder": true}),
        },
    );
    node_id
}

fn same_recovery_scope(candidate: &WorkExecution, failed: &WorkExecution) -> bool {
    if candidate.work_item_id == failed.work_item_id {
        return true;
    }
    match (
        candidate.target_service.as_deref(),
        failed.target_service.as_deref(),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn average(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

pub fn spawn_background_loops(service: ConductorService) {
    let discovery_interval =
        Duration::from_secs(service.config.discovery.refresh_interval_seconds.max(30));
    let planning_interval =
        Duration::from_secs(service.config.planning.refresh_interval_seconds.max(30));
    let execution_interval =
        Duration::from_secs(service.config.execution.refresh_interval_seconds.max(5));
    let refiner_sync_interval = Duration::from_secs(
        service
            .config
            .integrations
            .refiner
            .sync_interval_seconds
            .max(60),
    );
    let tracey_sync_interval = Duration::from_secs(
        service
            .config
            .integrations
            .tracey
            .sync_interval_seconds
            .max(60),
    );
    let atlassian_sync_interval = Duration::from_secs(
        service
            .config
            .integrations
            .atlassian
            .sync_interval_seconds
            .max(60),
    );

    let discovery_service = service.clone();
    tokio::spawn(async move {
        if let Err(error) = discovery_service.run_discovery_cycle().await {
            tracing::warn!(error = %error, "initial discovery cycle failed");
            let mut event = ConductorEvent::new(
                "discovery.failed",
                format!("initial discovery cycle failed: {}", error),
                json!({"error": error.to_string()}),
            );
            event.status = Some("failure".to_string());
            discovery_service.publish_event(event);
        }
        let mut ticker = tokio::time::interval(discovery_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = discovery_service.run_discovery_cycle().await {
                tracing::warn!(error = %error, "discovery cycle failed");
                let mut event = ConductorEvent::new(
                    "discovery.failed",
                    format!("discovery cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                discovery_service.publish_event(event);
            }
        }
    });

    let planning_service = service.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        if let Err(error) = planning_service.run_planning_cycle().await {
            tracing::warn!(error = %error, "initial planning cycle failed");
            let mut event = ConductorEvent::new(
                "planning.failed",
                format!("initial planning cycle failed: {}", error),
                json!({"error": error.to_string()}),
            );
            event.status = Some("failure".to_string());
            planning_service.publish_event(event);
        }
        let mut ticker = tokio::time::interval(planning_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = planning_service.run_planning_cycle().await {
                tracing::warn!(error = %error, "planning cycle failed");
                let mut event = ConductorEvent::new(
                    "planning.failed",
                    format!("planning cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                planning_service.publish_event(event);
            }
        }
    });

    let execution_service = service.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        if let Err(error) = execution_service.run_execution_cycle().await {
            tracing::warn!(error = %error, "initial execution cycle failed");
            let mut event = ConductorEvent::new(
                "execution.cycle.failed",
                format!("initial execution cycle failed: {}", error),
                json!({"error": error.to_string()}),
            );
            event.status = Some("failure".to_string());
            execution_service.publish_event(event);
        }
        let mut ticker = tokio::time::interval(execution_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = execution_service.run_execution_cycle().await {
                tracing::warn!(error = %error, "execution cycle failed");
                let mut event = ConductorEvent::new(
                    "execution.cycle.failed",
                    format!("execution cycle failed: {}", error),
                    json!({"error": error.to_string()}),
                );
                event.status = Some("failure".to_string());
                execution_service.publish_event(event);
            }
        }
    });

    if atlassian_sync_is_configured(service.config.as_ref())
        && service.config.integrations.atlassian.sync_interval_seconds > 0
    {
        let atlassian_service = service.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            if let Err(error) = atlassian_service
                .sync_all_links(TraceabilitySyncRequest {
                    systems: vec!["jira".to_string(), "confluence".to_string()],
                })
                .await
            {
                tracing::warn!(error = %error, "initial atlassian sync failed");
                let mut event = ConductorEvent::new(
                    "traceability.sync.failed",
                    format!("initial atlassian sync failed: {}", error),
                    json!({"error": error.to_string(), "systems": ["jira", "confluence"]}),
                );
                event.status = Some("failure".to_string());
                atlassian_service.publish_event(event);
            }
            let mut ticker = tokio::time::interval(atlassian_sync_interval);
            loop {
                ticker.tick().await;
                if let Err(error) = atlassian_service
                    .sync_all_links(TraceabilitySyncRequest {
                        systems: vec!["jira".to_string(), "confluence".to_string()],
                    })
                    .await
                {
                    tracing::warn!(error = %error, "atlassian sync failed");
                    let mut event = ConductorEvent::new(
                        "traceability.sync.failed",
                        format!("atlassian sync failed: {}", error),
                        json!({"error": error.to_string(), "systems": ["jira", "confluence"]}),
                    );
                    event.status = Some("failure".to_string());
                    atlassian_service.publish_event(event);
                }
            }
        });
    }

    if refiner_sync_is_enabled(service.config.as_ref()) {
        let refiner_service = service.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if let Err(error) = refiner_service
                .sync_all_links(TraceabilitySyncRequest {
                    systems: vec!["refiner".to_string()],
                })
                .await
            {
                tracing::warn!(error = %error, "initial refiner sync failed");
                let mut event = ConductorEvent::new(
                    "traceability.sync.failed",
                    format!("initial refiner sync failed: {}", error),
                    json!({"error": error.to_string(), "systems": ["refiner"]}),
                );
                event.status = Some("failure".to_string());
                refiner_service.publish_event(event);
            }
            let mut ticker = tokio::time::interval(refiner_sync_interval);
            loop {
                ticker.tick().await;
                if let Err(error) = refiner_service
                    .sync_all_links(TraceabilitySyncRequest {
                        systems: vec!["refiner".to_string()],
                    })
                    .await
                {
                    tracing::warn!(error = %error, "refiner sync failed");
                    let mut event = ConductorEvent::new(
                        "traceability.sync.failed",
                        format!("refiner sync failed: {}", error),
                        json!({"error": error.to_string(), "systems": ["refiner"]}),
                    );
                    event.status = Some("failure".to_string());
                    refiner_service.publish_event(event);
                }
            }
        });
    }

    if tracey_sync_is_enabled(service.config.as_ref()) {
        let tracey_service = service.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(25)).await;
            if let Err(error) = tracey_service
                .sync_all_links(TraceabilitySyncRequest {
                    systems: vec!["tracey".to_string()],
                })
                .await
            {
                tracing::warn!(error = %error, "initial tracey sync failed");
                let mut event = ConductorEvent::new(
                    "traceability.sync.failed",
                    format!("initial tracey sync failed: {}", error),
                    json!({"error": error.to_string(), "systems": ["tracey"]}),
                );
                event.status = Some("failure".to_string());
                tracey_service.publish_event(event);
            }
            let mut ticker = tokio::time::interval(tracey_sync_interval);
            loop {
                ticker.tick().await;
                if let Err(error) = tracey_service
                    .sync_all_links(TraceabilitySyncRequest {
                        systems: vec!["tracey".to_string()],
                    })
                    .await
                {
                    tracing::warn!(error = %error, "tracey sync failed");
                    let mut event = ConductorEvent::new(
                        "traceability.sync.failed",
                        format!("tracey sync failed: {}", error),
                        json!({"error": error.to_string(), "systems": ["tracey"]}),
                    );
                    event.status = Some("failure".to_string());
                    tracey_service.publish_event(event);
                }
            }
        });
    }
}

fn traceability_finding(work_item: &WorkItem, findings: &[FindingRecord]) -> Option<FindingRecord> {
    let finding_id = work_item
        .plan
        .get("finding_id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok());
    let finding_key = work_item
        .plan
        .get("finding_key")
        .and_then(|value| value.as_str());

    findings
        .iter()
        .find(|finding| {
            finding_id.is_some_and(|id| finding.id == id)
                || finding_key.is_some_and(|key| finding.finding_key == key)
        })
        .cloned()
}

fn traceability_service(
    work_item: &WorkItem,
    services: &[ServiceSnapshot],
) -> Option<ServiceSnapshot> {
    work_item
        .target_service
        .as_deref()
        .and_then(|target| {
            services
                .iter()
                .find(|service| service.service_key == target)
        })
        .cloned()
}

fn traceability_repository(
    finding: &Option<FindingRecord>,
    service: Option<&ServiceSnapshot>,
    repositories: &[RepositorySnapshot],
) -> Option<RepositorySnapshot> {
    if let Some(repository_key) = finding
        .as_ref()
        .and_then(|finding| finding.target_repository.as_deref())
    {
        if let Some(repository) = repositories
            .iter()
            .find(|repository| repository.repo_key == repository_key)
        {
            return Some(repository.clone());
        }
    }

    let Some(service) = service else {
        return None;
    };

    if let Some(repo_path) = service.repo_path.as_deref() {
        if let Some(repository) = repositories
            .iter()
            .find(|repository| repository.local_path.as_deref() == Some(repo_path))
        {
            return Some(repository.clone());
        }
    }

    repositories
        .iter()
        .find(|repository| repository.linked_services.contains(&service.service_key))
        .cloned()
}

fn traceability_independent_validation(execution: &WorkExecution) -> Option<serde_json::Value> {
    execution
        .verification
        .get("independent_validation")
        .cloned()
        .or_else(|| {
            execution
                .latest_payload
                .get("independent_validation")
                .cloned()
        })
}

fn execution_sort_key(execution: &WorkExecution) -> chrono::DateTime<chrono::Utc> {
    execution.finished_at.unwrap_or(execution.updated_at)
}

fn resolve_finding_key(
    request_finding_key: Option<String>,
    traceability: &WorkItemTraceability,
) -> Option<String> {
    request_finding_key
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            traceability
                .finding
                .as_ref()
                .map(|finding| finding.finding_key.clone())
        })
        .or_else(|| {
            traceability
                .work_item
                .plan
                .get("finding_key")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
}

fn jira_reference_type(issue_type: &str) -> String {
    let issue_type = issue_type.trim();
    if issue_type.eq_ignore_ascii_case("bug") {
        "bug".to_string()
    } else if issue_type.eq_ignore_ascii_case("epic") {
        "epic".to_string()
    } else if issue_type.eq_ignore_ascii_case("story") {
        "story".to_string()
    } else {
        "task".to_string()
    }
}

fn build_jira_labels(
    traceability: &WorkItemTraceability,
    extra_labels: Vec<String>,
) -> Vec<String> {
    let mut labels = vec![
        "conductor".to_string(),
        work_item_label(&traceability.work_item),
        format!(
            "stage-{}",
            sanitize_external_label(traceability.work_item.delivery_stage.as_str())
        ),
    ];
    if let Some(service) = traceability.work_item.target_service.as_deref() {
        labels.push(format!("service-{}", sanitize_external_label(service)));
    }
    if let Some(finding) = traceability.finding.as_ref() {
        labels.push(format!(
            "finding-{}",
            sanitize_external_label(&finding.finding_key)
        ));
        labels.push(format!(
            "severity-{}",
            sanitize_external_label(finding.severity.as_str())
        ));
    }
    labels.extend(
        extra_labels
            .into_iter()
            .map(|value| sanitize_external_label(&value)),
    );
    unique_strings(
        labels
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect(),
    )
}

fn build_confluence_labels(
    traceability: &WorkItemTraceability,
    extra_labels: Vec<String>,
) -> Vec<String> {
    build_jira_labels(traceability, extra_labels)
}

fn work_item_label(item: &WorkItem) -> String {
    sanitize_external_label(&format!("conductor-work-item-{}", item.id))
}

fn sanitize_external_label(value: &str) -> String {
    let mut label = value
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while label.contains("--") {
        label = label.replace("--", "-");
    }
    label.trim_matches('-').chars().take(80).collect()
}

fn escape_jql_literal(value: &str) -> String {
    value.trim().replace('"', "\\\"")
}

fn jira_issue_summary(traceability: &WorkItemTraceability) -> String {
    let prefix = traceability
        .finding
        .as_ref()
        .map(|finding| finding.category.as_str())
        .unwrap_or("improvement");
    format!(
        "[{}] {}",
        prefix.trim(),
        traceability.work_item.title.trim()
    )
}

fn jira_issue_description(
    traceability: &WorkItemTraceability,
    public_base_url: Option<&str>,
) -> String {
    let mut lines = vec![
        format!("Work item: {}", traceability.work_item.title.trim()),
        format!("Summary: {}", traceability.work_item.summary.trim()),
        format!(
            "Delivery stage: {}",
            traceability.work_item.delivery_stage.as_str()
        ),
        format!(
            "Rollout strategy: {}",
            traceability.work_item.rollout_strategy.as_str()
        ),
        format!("Priority: {}", traceability.work_item.priority),
        format!("Progress: {}%", traceability.work_item.progress_pct),
    ];
    if let Some(service) = traceability.work_item.target_service.as_deref() {
        lines.push(format!("Target service: {}", service));
    }
    if let Some(finding) = traceability.finding.as_ref() {
        lines.push(String::new());
        lines.push(format!("Finding: {}", finding.title.trim()));
        lines.push(format!("Finding key: {}", finding.finding_key.trim()));
        lines.push(format!("Severity: {}", finding.severity.as_str()));
        lines.push(format!("Category: {}", finding.category.trim()));
        lines.push(format!("Finding summary: {}", finding.summary.trim()));
    }
    if !traceability.evidence.is_empty() {
        lines.push(String::new());
        lines.push("Evidence:".to_string());
        for evidence in traceability.evidence.iter().take(6) {
            lines.push(format!(
                "- [{}:{}] {}",
                evidence.source_kind, evidence.evidence_type, evidence.summary
            ));
        }
    }
    if !traceability.executions.is_empty() {
        lines.push(String::new());
        lines.push("Recent executions:".to_string());
        for execution in traceability.executions.iter().take(3) {
            lines.push(format!(
                "- {} {} {}",
                execution.delivery_stage.as_str(),
                execution.rollout_strategy.as_str(),
                execution.status.as_str()
            ));
        }
    }
    if let Some(summary) = traceability
        .independent_validation
        .get("summary")
        .and_then(Value::as_str)
    {
        lines.push(String::new());
        lines.push(format!("Independent validation: {}", summary.trim()));
    }
    if let Some(url) = public_base_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        lines.push(String::new());
        lines.push(format!(
            "Conductor traceability: {}/api/v1/work-items/{}/traceability",
            url.trim_end_matches('/'),
            traceability.work_item.id
        ));
    }
    lines.join("\n")
}

fn confluence_page_title(traceability: &WorkItemTraceability) -> String {
    format!(
        "Conductor Work Item {} {}",
        traceability.work_item.id,
        traceability.work_item.title.trim()
    )
}

fn confluence_page_body(
    traceability: &WorkItemTraceability,
    public_base_url: Option<&str>,
) -> String {
    let mut html = String::new();
    html.push_str("<h1>Conductor Work Item</h1>");
    html.push_str("<table><tbody>");
    append_table_row(&mut html, "Work item", &traceability.work_item.title);
    append_table_row(&mut html, "Summary", &traceability.work_item.summary);
    append_table_row(
        &mut html,
        "Delivery stage",
        traceability.work_item.delivery_stage.as_str(),
    );
    append_table_row(
        &mut html,
        "Rollout strategy",
        traceability.work_item.rollout_strategy.as_str(),
    );
    append_table_row(&mut html, "Status", traceability.work_item.status.as_str());
    append_table_row(
        &mut html,
        "Progress",
        &format!("{}%", traceability.work_item.progress_pct),
    );
    if let Some(service) = traceability.work_item.target_service.as_deref() {
        append_table_row(&mut html, "Target service", service);
    }
    html.push_str("</tbody></table>");

    if let Some(finding) = traceability.finding.as_ref() {
        html.push_str("<h2>Finding</h2><table><tbody>");
        append_table_row(&mut html, "Title", &finding.title);
        append_table_row(&mut html, "Key", &finding.finding_key);
        append_table_row(&mut html, "Severity", finding.severity.as_str());
        append_table_row(&mut html, "Category", &finding.category);
        append_table_row(&mut html, "Summary", &finding.summary);
        html.push_str("</tbody></table>");
    }

    if !traceability.evidence.is_empty() {
        html.push_str("<h2>Evidence</h2><ul>");
        for evidence in traceability.evidence.iter().take(10) {
            html.push_str("<li>");
            html.push_str(&escape_html(&format!(
                "[{}:{}] {}",
                evidence.source_kind, evidence.evidence_type, evidence.summary
            )));
            html.push_str("</li>");
        }
        html.push_str("</ul>");
    }

    if !traceability.executions.is_empty() {
        html.push_str("<h2>Executions</h2><ul>");
        for execution in traceability.executions.iter().take(10) {
            html.push_str("<li>");
            html.push_str(&escape_html(&format!(
                "{} / {} / {}",
                execution.delivery_stage.as_str(),
                execution.rollout_strategy.as_str(),
                execution.status.as_str()
            )));
            html.push_str("</li>");
        }
        html.push_str("</ul>");
    }

    if !traceability.links.is_empty() {
        html.push_str("<h2>Existing Links</h2><ul>");
        for link in &traceability.links {
            html.push_str("<li>");
            html.push_str(&escape_html(&format!(
                "{} {} {}",
                link.system, link.reference_type, link.reference_key
            )));
            html.push_str("</li>");
        }
        html.push_str("</ul>");
    }

    if !traceability.independent_validation.is_null()
        && traceability.independent_validation != json!({})
    {
        html.push_str("<h2>Independent Validation</h2><pre>");
        html.push_str(&escape_html(
            &serde_json::to_string_pretty(&traceability.independent_validation)
                .unwrap_or_else(|_| "{}".to_string()),
        ));
        html.push_str("</pre>");
    }

    if let Some(url) = public_base_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        html.push_str("<h2>Conductor Links</h2><ul>");
        html.push_str("<li>");
        html.push_str(&escape_html(&format!(
            "{}/dashboard",
            url.trim_end_matches('/')
        )));
        html.push_str("</li><li>");
        html.push_str(&escape_html(&format!(
            "{}/api/v1/work-items/{}/traceability",
            url.trim_end_matches('/'),
            traceability.work_item.id
        )));
        html.push_str("</li></ul>");
    }

    html
}

fn append_table_row(html: &mut String, label: &str, value: &str) {
    html.push_str("<tr><th>");
    html.push_str(&escape_html(label));
    html.push_str("</th><td>");
    html.push_str(&escape_html(value));
    html.push_str("</td></tr>");
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn normalize_requested_systems(values: Vec<String>) -> Vec<String> {
    unique_strings(
        values
            .into_iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .collect(),
    )
}

fn link_metadata(existing: &Value, overlay: Value) -> Value {
    let mut metadata = existing.as_object().cloned().unwrap_or_default();
    metadata.remove("last_sync_error");
    metadata.remove("last_sync_error_at");
    if let Some(overlay) = overlay.as_object() {
        for (key, value) in overlay {
            metadata.insert(key.clone(), value.clone());
        }
    }
    Value::Object(metadata)
}

fn sync_error_metadata(existing: &Value, error: &str) -> Value {
    let mut metadata = existing.as_object().cloned().unwrap_or_default();
    metadata.insert(
        "last_sync_error".to_string(),
        Value::String(error.trim().to_string()),
    );
    metadata.insert(
        "last_sync_error_at".to_string(),
        Value::String(now_utc().to_rfc3339()),
    );
    Value::Object(metadata)
}

fn build_refiner_traceability_requests(
    client: Option<&RefinerClient>,
    work_item: &WorkItem,
    execution: &WorkExecution,
    job_detail: Option<&Value>,
    requirements_progress: Option<&Value>,
    requirements_summary: Option<&Value>,
    workspace_payload: Option<&Value>,
    sync_error: Option<&str>,
) -> Vec<NewTraceabilityLink> {
    let Some(job_id) = execution.refiner_job_id.as_deref() else {
        return Vec::new();
    };

    let project_name = json_string_at_opt(job_detail, &["project_name"])
        .or_else(|| json_string_at(&execution.request_payload, &["project_name"]))
        .unwrap_or_else(|| format!("Refiner job {}", job_id));
    let workflow = json_string_at_opt(job_detail, &["workflow"])
        .or_else(|| json_string_at(&execution.request_payload, &["workflow"]))
        .unwrap_or_else(|| "project_solver".to_string());
    let job_status = json_string_at_opt(job_detail, &["status"])
        .unwrap_or_else(|| execution.status.as_str().to_string());
    let progress = json_value_at_opt(job_detail, &["progress"])
        .cloned()
        .unwrap_or_else(|| {
            json!(
                if matches!(execution.status, crate::models::ExecutionStatus::Success) {
                    100
                } else {
                    0
                }
            )
        });
    let delivery_stage = execution
        .request_payload
        .get("delivery_stage")
        .and_then(Value::as_str)
        .unwrap_or(execution.delivery_stage.as_str());
    let rollout_strategy = execution
        .request_payload
        .get("rollout_strategy")
        .and_then(Value::as_str)
        .unwrap_or(execution.rollout_strategy.as_str());
    let validated_stages = execution
        .request_payload
        .get("validated_stages")
        .cloned()
        .unwrap_or_else(|| {
            json!(
                work_item
                    .validated_stages
                    .iter()
                    .map(|stage| stage.as_str())
                    .collect::<Vec<_>>()
            )
        });
    let canary_percentage = execution
        .request_payload
        .get("rollout")
        .and_then(|value| value.get("canary_percentage"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let project_root = json_string_at_opt(job_detail, &["payload", "project_root"])
        .or_else(|| json_string_at(&execution.request_payload, &["project_root"]));
    let repo_url = json_string_at_opt(job_detail, &["payload", "repo_url"])
        .or_else(|| json_string_at(&execution.request_payload, &["repo_url"]));
    let repo_branch = json_string_at_opt(job_detail, &["payload", "repo_branch"])
        .or_else(|| json_string_at(&execution.request_payload, &["repo_branch"]));
    let work_branch = json_string_at_opt(job_detail, &["payload", "work_branch"])
        .or_else(|| json_string_at(&execution.request_payload, &["work_branch"]));
    let output_paths = json_value_at_opt(job_detail, &["output_paths"])
        .cloned()
        .unwrap_or_else(|| json!({}));
    let primary_output = output_paths
        .get("primary")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let repo_info = json_value_at_opt(job_detail, &["repo_info"])
        .cloned()
        .unwrap_or_else(|| json!({}));
    let workspace_env = workspace_payload
        .and_then(|value| value.get("workspace"))
        .cloned()
        .or_else(|| json_value_at_opt(job_detail, &["workspace_env"]).cloned())
        .unwrap_or_else(|| json!({}));
    let workspace_status = json_string_at(&workspace_env, &["status"]).or_else(|| {
        if project_root.is_some() || repo_info.get("workspace").and_then(Value::as_str).is_some() {
            Some("available".to_string())
        } else {
            None
        }
    });
    let workspace_url = json_string_at(&workspace_env, &["ide_url"])
        .or_else(|| client.map(|value| value.workspace_url(job_id)));
    let requirements_status = json_string_at_opt(requirements_progress, &["status"])
        .unwrap_or_else(|| {
            if requirements_summary
                .and_then(|value| value.get("total"))
                .and_then(Value::as_u64)
                .unwrap_or_default()
                > 0
            {
                "ready".to_string()
            } else {
                "pending".to_string()
            }
        });
    let finding_key = work_item_finding_key(work_item);
    let stages = compact_refiner_stages(job_detail.and_then(|value| value.get("stages")));
    let requirements_progress_summary = compact_requirements_progress(requirements_progress);
    let requirements_summary_compact = compact_requirements_summary(requirements_summary);
    let job_url = client.map(|value| value.job_url(job_id));
    let requirements_url = client.map(|value| value.requirements_summary_url(job_id));
    let rollout_status = derive_refiner_rollout_status(&job_status, execution);

    let job_metadata = json!({
        "source": "refiner_job",
        "job_id": job_id,
        "workflow": workflow,
        "progress": progress,
        "project_root": project_root,
        "repo_url": repo_url,
        "repo_branch": repo_branch,
        "work_branch": work_branch,
        "delivery_stage": delivery_stage,
        "rollout_strategy": rollout_strategy,
        "validated_stages": validated_stages,
        "stages": stages,
        "output_paths": output_paths,
        "repo_info": compact_refiner_repo_info(&repo_info),
        "workspace": compact_refiner_workspace(&workspace_env),
        "live_detail": job_detail.is_some(),
    });
    let build_metadata = json!({
        "source": "refiner_build",
        "job_id": job_id,
        "workflow": workflow,
        "project_name": project_name,
        "primary_output": primary_output,
        "output_paths": output_paths,
        "stages": stages,
        "execution_status": execution.status.as_str(),
        "delivery_stage": delivery_stage,
        "rollout_strategy": rollout_strategy,
    });
    let requirements_metadata = json!({
        "source": "refiner_requirements",
        "job_id": job_id,
        "workflow": workflow,
        "delivery_stage": delivery_stage,
        "rollout_strategy": rollout_strategy,
        "progress": requirements_progress_summary,
        "summary": requirements_summary_compact,
    });
    let workspace_metadata = json!({
        "source": "refiner_workspace",
        "job_id": job_id,
        "project_root": project_root,
        "repo_url": repo_url,
        "repo_branch": repo_branch,
        "work_branch": work_branch,
        "repo_info": compact_refiner_repo_info(&repo_info),
        "workspace": compact_refiner_workspace(&workspace_env),
    });
    let rollout_metadata = json!({
        "source": "refiner_rollout",
        "job_id": job_id,
        "delivery_stage": delivery_stage,
        "rollout_strategy": rollout_strategy,
        "canary_percentage": canary_percentage,
        "validated_stages": validated_stages,
        "job_status": job_status,
        "execution_status": execution.status.as_str(),
        "verification_passed": execution.verification.get("passed").and_then(Value::as_bool),
        "stages": stages,
    });

    let apply_error = |metadata: Value| {
        if let Some(error) = sync_error {
            sync_error_metadata(&metadata, error)
        } else {
            metadata
        }
    };

    vec![
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "refiner".to_string(),
            reference_type: "job".to_string(),
            reference_key: job_id.to_string(),
            title: Some(project_name.clone()),
            status: Some(job_status.clone()),
            url: job_url.clone(),
            metadata: apply_error(job_metadata),
        },
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "refiner".to_string(),
            reference_type: "build".to_string(),
            reference_key: job_id.to_string(),
            title: Some(format!("Refiner build {}", project_name)),
            status: Some(job_status.clone()),
            url: job_url.clone(),
            metadata: apply_error(build_metadata),
        },
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "refiner".to_string(),
            reference_type: "requirements".to_string(),
            reference_key: job_id.to_string(),
            title: Some(format!("Requirements for {}", project_name)),
            status: Some(requirements_status),
            url: requirements_url,
            metadata: apply_error(requirements_metadata),
        },
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "refiner".to_string(),
            reference_type: "workspace".to_string(),
            reference_key: job_id.to_string(),
            title: Some(format!("Workspace for {}", project_name)),
            status: workspace_status,
            url: workspace_url,
            metadata: apply_error(workspace_metadata),
        },
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key,
            system: "refiner".to_string(),
            reference_type: "rollout".to_string(),
            reference_key: job_id.to_string(),
            title: Some(format!("{} rollout for {}", rollout_strategy, project_name)),
            status: Some(rollout_status),
            url: job_url,
            metadata: apply_error(rollout_metadata),
        },
    ]
}

fn build_tracey_traceability_requests(
    client: Option<&TraceyClient>,
    work_item: &WorkItem,
    execution: &WorkExecution,
    status: Option<&Value>,
    loader_status: Option<&Value>,
    tracey_service: Option<&ServiceSnapshot>,
) -> Vec<NewTraceabilityLink> {
    if status.is_none() && loader_status.is_none() && tracey_service.is_none() {
        return Vec::new();
    }

    let finding_key = work_item_finding_key(work_item);
    let agent_id = json_string_at_opt(loader_status, &["agent_id"])
        .or_else(|| json_string_at_opt(status, &["agent_id"]))
        .or_else(|| tracey_service.map(|service| service.display_name.clone()))
        .unwrap_or_else(|| "tracey".to_string());
    let version = json_string_at_opt(loader_status, &["version"])
        .or_else(|| json_string_at_opt(status, &["agent_version"]));
    let channel = json_string_at_opt(loader_status, &["channel"]);
    let pending_rollback = json_bool_at_opt(loader_status, &["pending_rollback"]).unwrap_or(false);
    let distributable = json_bool_at_opt(loader_status, &["distributable"]).unwrap_or(false);
    let rollback_previous_version =
        json_string_at_opt(loader_status, &["rollback_previous_version"]);
    let blocked_provider_count = json_value_at_opt(
        loader_status,
        &["loader_threats", "summary", "blocked_provider_count"],
    )
    .or_else(|| {
        json_value_at_opt(
            status,
            &["loader_threats", "summary", "blocked_provider_count"],
        )
    })
    .and_then(Value::as_u64)
    .unwrap_or_default();
    let blocked_artifact_count = json_value_at_opt(
        loader_status,
        &["loader_threats", "summary", "blocked_artifact_count"],
    )
    .or_else(|| {
        json_value_at_opt(
            status,
            &["loader_threats", "summary", "blocked_artifact_count"],
        )
    })
    .and_then(Value::as_u64)
    .unwrap_or_default();
    let highest_provider_risk = json_value_at_opt(
        loader_status,
        &["loader_threats", "summary", "highest_provider_risk"],
    )
    .or_else(|| {
        json_value_at_opt(
            status,
            &["loader_threats", "summary", "highest_provider_risk"],
        )
    })
    .and_then(Value::as_f64)
    .unwrap_or_default();
    let highest_artifact_risk = json_value_at_opt(
        loader_status,
        &["loader_threats", "summary", "highest_artifact_risk"],
    )
    .or_else(|| {
        json_value_at_opt(
            status,
            &["loader_threats", "summary", "highest_artifact_risk"],
        )
    })
    .and_then(Value::as_f64)
    .unwrap_or_default();
    let runtime_status = json_string_at_opt(status, &["status"])
        .or_else(|| {
            tracey_service.and_then(|service| {
                service
                    .probe
                    .get("health")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
        })
        .unwrap_or_else(|| "observed".to_string());
    let posture = json_string_at_opt(status, &["posture"]);
    let rollout_status = if pending_rollback {
        "rollback_pending".to_string()
    } else if distributable {
        "distributable".to_string()
    } else if loader_status.is_some() {
        "restricted".to_string()
    } else {
        execution.status.as_str().to_string()
    };
    let loader_threat_status = if blocked_provider_count > 0 || blocked_artifact_count > 0 {
        Some("blocked".to_string())
    } else if highest_provider_risk >= 0.55 || highest_artifact_risk >= 0.55 {
        Some("warning".to_string())
    } else {
        None
    };
    let runtime_url = client.map(TraceyClient::status_url);
    let loader_url = client
        .map(TraceyClient::loader_status_url)
        .or_else(|| runtime_url.clone());
    let source_kind = if client.is_some() {
        "live_status"
    } else {
        "probe_snapshot"
    };
    let status_excerpt = compact_tracey_status(status, tracey_service, source_kind);
    let loader_excerpt = compact_tracey_loader(loader_status);
    let threat_excerpt = compact_tracey_loader_threat_summary(status, loader_status);

    let mut requests = vec![
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "tracey".to_string(),
            reference_type: "runtime".to_string(),
            reference_key: format!("runtime:{}", agent_id),
            title: Some(format!("Tracey runtime {}", agent_id)),
            status: Some(posture.clone().unwrap_or(runtime_status.clone())),
            url: runtime_url.clone(),
            metadata: json!({
                "source": source_kind,
                "status": status_excerpt,
                "loader": loader_excerpt,
                "delivery_stage": execution.delivery_stage.as_str(),
                "rollout_strategy": execution.rollout_strategy.as_str(),
            }),
        },
        NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "tracey".to_string(),
            reference_type: "rollout".to_string(),
            reference_key: format!(
                "loader:{}",
                version.clone().unwrap_or_else(|| agent_id.clone())
            ),
            title: Some(format!(
                "Tracey rollout {}",
                version.clone().unwrap_or_else(|| agent_id.clone())
            )),
            status: Some(rollout_status),
            url: loader_url.clone(),
            metadata: json!({
                "source": source_kind,
                "version": version,
                "channel": channel,
                "pending_rollback": pending_rollback,
                "rollback_previous_version": rollback_previous_version,
                "distributable": distributable,
                "status": status_excerpt,
                "loader": loader_excerpt,
                "loader_threats": threat_excerpt,
                "delivery_stage": execution.delivery_stage.as_str(),
                "rollout_strategy": execution.rollout_strategy.as_str(),
            }),
        },
    ];

    if let Some(status) = loader_threat_status {
        requests.push(NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "tracey".to_string(),
            reference_type: "loader_threat".to_string(),
            reference_key: format!("loader-threat:{}", agent_id),
            title: Some("Tracey loader threat state".to_string()),
            status: Some(status),
            url: loader_url.clone(),
            metadata: json!({
                "source": source_kind,
                "summary": threat_excerpt,
                "status": status_excerpt,
                "delivery_stage": execution.delivery_stage.as_str(),
                "rollout_strategy": execution.rollout_strategy.as_str(),
            }),
        });
    }

    if pending_rollback {
        requests.push(NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key: finding_key.clone(),
            system: "tracey".to_string(),
            reference_type: "rollback".to_string(),
            reference_key: format!(
                "rollback:{}",
                rollback_previous_version
                    .clone()
                    .unwrap_or_else(|| version.clone().unwrap_or_else(|| agent_id.clone()))
            ),
            title: Some(
                rollback_previous_version
                    .as_ref()
                    .map(|value| format!("Tracey rollback pending to {}", value))
                    .unwrap_or_else(|| "Tracey rollback pending".to_string()),
            ),
            status: Some("pending".to_string()),
            url: loader_url.clone(),
            metadata: json!({
                "source": source_kind,
                "version": version,
                "rollback_previous_version": rollback_previous_version,
                "loader": loader_excerpt,
                "loader_threats": threat_excerpt,
                "status": status_excerpt,
            }),
        });
    }

    if pending_rollback || blocked_provider_count > 0 || blocked_artifact_count > 0 {
        requests.push(NewTraceabilityLink {
            execution_id: Some(execution.id),
            finding_key,
            system: "tracey".to_string(),
            reference_type: "incident".to_string(),
            reference_key: if pending_rollback {
                format!(
                    "incident:rollback:{}",
                    rollback_previous_version
                        .clone()
                        .unwrap_or_else(|| version.clone().unwrap_or_else(|| agent_id.clone()))
                )
            } else {
                format!("incident:loader-threat:{}", agent_id)
            },
            title: Some(if pending_rollback {
                "Tracey rollback incident".to_string()
            } else {
                "Tracey loader threat incident".to_string()
            }),
            status: Some("open".to_string()),
            url: loader_url,
            metadata: json!({
                "source": source_kind,
                "pending_rollback": pending_rollback,
                "rollback_previous_version": rollback_previous_version,
                "summary": threat_excerpt,
                "status": status_excerpt,
                "loader": loader_excerpt,
                "delivery_stage": execution.delivery_stage.as_str(),
                "rollout_strategy": execution.rollout_strategy.as_str(),
            }),
        });
    }

    requests
}

fn compact_refiner_workspace(value: &Value) -> Value {
    json!({
        "provider": value.get("provider"),
        "status": value.get("status"),
        "vm_id": value.get("vm_id"),
        "ide_url": value.get("ide_url"),
        "preview_url": value.get("preview_url"),
        "details": value.get("details"),
        "requested_at": value.get("requested_at"),
        "updated_at": value.get("updated_at"),
    })
}

fn compact_refiner_repo_info(value: &Value) -> Value {
    json!({
        "workspace": value.get("workspace"),
        "branch": value.get("branch"),
        "owner": value.get("owner"),
        "repo": value.get("repo"),
        "project_root": value.get("project_root"),
    })
}

fn compact_refiner_stages(stages: Option<&Value>) -> Value {
    let items = stages
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .take(12)
                .map(|entry| {
                    json!({
                        "name": entry.get("name"),
                        "status": entry.get("status"),
                        "started_at": entry.get("started_at"),
                        "finished_at": entry.get("finished_at"),
                        "message": entry.get("message"),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!(items)
}

fn compact_requirements_progress(progress: Option<&Value>) -> Value {
    let Some(progress) = progress else {
        return json!({});
    };
    json!({
        "total": progress.get("total"),
        "completed": progress.get("completed"),
        "in_progress": progress.get("in_progress"),
        "remaining": progress.get("remaining"),
        "status": progress.get("status"),
        "source": progress.get("source"),
        "message": progress.get("message"),
        "updated_at": progress.get("updated_at"),
    })
}

fn compact_requirements_summary(summary: Option<&Value>) -> Value {
    let Some(summary) = summary else {
        return json!({});
    };
    let items = summary
        .get("items")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().take(10).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    json!({
        "summary": summary.get("summary"),
        "total": summary.get("total"),
        "source": summary.get("source"),
        "updated_at": summary.get("updated_at"),
        "message": summary.get("message"),
        "redacted": summary.get("redacted"),
        "items_preview": items,
    })
}

fn compact_tracey_status(
    status: Option<&Value>,
    tracey_service: Option<&ServiceSnapshot>,
    source_kind: &str,
) -> Value {
    let probe = tracey_service
        .map(|service| service.probe.clone())
        .unwrap_or_else(|| json!({}));
    json!({
        "source": source_kind,
        "ts_ms": status.and_then(|value| value.get("ts_ms")),
        "agent_id": status.and_then(|value| value.get("agent_id")),
        "agent_version": status.and_then(|value| value.get("agent_version")),
        "status": status.and_then(|value| value.get("status")),
        "posture": status.and_then(|value| value.get("posture")),
        "continuum_loop": status.and_then(|value| value.get("continuum_loop")).map(|loop_status| {
            json!({
                "mode": loop_status.get("mode"),
                "next_action": loop_status.get("next_action"),
                "overall_score": loop_status.get("overall_score"),
                "readiness_score": loop_status.get("readiness_score"),
                "placement_score": loop_status.get("placement_score"),
                "compromise_risk": loop_status.get("compromise_risk"),
                "fuzzy_confidence": loop_status.get("fuzzy_confidence"),
            })
        }),
        "probe_health": probe.get("health"),
        "probe_summary": probe.get("summary"),
    })
}

fn compact_tracey_loader(loader_status: Option<&Value>) -> Value {
    let Some(loader_status) = loader_status else {
        return json!({});
    };
    json!({
        "ts_ms": loader_status.get("ts_ms"),
        "agent_id": loader_status.get("agent_id"),
        "version": loader_status.get("version"),
        "channel": loader_status.get("channel"),
        "distributable": loader_status.get("distributable"),
        "transfer_addr": loader_status.get("transfer_addr"),
        "pending_rollback": loader_status.get("pending_rollback"),
        "rollback_previous_version": loader_status.get("rollback_previous_version"),
    })
}

fn compact_tracey_loader_threat_summary(
    status: Option<&Value>,
    loader_status: Option<&Value>,
) -> Value {
    let summary = json_value_at_opt(loader_status, &["loader_threats", "summary"])
        .or_else(|| json_value_at_opt(status, &["loader_threats", "summary"]));
    let Some(summary) = summary else {
        return json!({});
    };
    json!({
        "blocked_provider_count": summary.get("blocked_provider_count"),
        "blocked_artifact_count": summary.get("blocked_artifact_count"),
        "highest_provider_risk": summary.get("highest_provider_risk"),
        "highest_artifact_risk": summary.get("highest_artifact_risk"),
        "remote_reporters": summary.get("remote_reporters"),
    })
}

fn derive_refiner_rollout_status(job_status: &str, execution: &WorkExecution) -> String {
    if execution
        .verification
        .get("passed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || matches!(execution.status, crate::models::ExecutionStatus::Success)
    {
        return "verified".to_string();
    }
    if matches!(
        execution.status,
        crate::models::ExecutionStatus::Failure
            | crate::models::ExecutionStatus::Blocked
            | crate::models::ExecutionStatus::Cancelled
    ) || matches!(
        job_status.trim().to_ascii_lowercase().as_str(),
        "failed" | "blocked" | "cancelled" | "stopped"
    ) {
        return "failed".to_string();
    }
    if matches!(
        execution.status,
        crate::models::ExecutionStatus::Pending
            | crate::models::ExecutionStatus::Planning
            | crate::models::ExecutionStatus::Submitted
            | crate::models::ExecutionStatus::Running
            | crate::models::ExecutionStatus::Verifying
    ) || matches!(
        job_status.trim().to_ascii_lowercase().as_str(),
        "queued" | "running" | "paused" | "submitted" | "completed"
    ) {
        return "in_progress".to_string();
    }
    job_status.trim().to_ascii_lowercase()
}

fn work_item_finding_key(work_item: &WorkItem) -> Option<String> {
    work_item
        .plan
        .get("finding_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn targets_service(
    service_key: &str,
    execution: Option<&WorkExecution>,
    work_item: Option<&WorkItem>,
) -> bool {
    execution
        .and_then(|value| value.target_service.as_deref())
        .is_some_and(|value| value.eq_ignore_ascii_case(service_key))
        || work_item
            .and_then(|value| value.target_service.as_deref())
            .is_some_and(|value| value.eq_ignore_ascii_case(service_key))
}

fn json_value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current)
}

fn json_value_at_opt<'a>(value: Option<&'a Value>, path: &[&str]) -> Option<&'a Value> {
    value.and_then(|item| json_value_at(item, path))
}

fn json_string_at(value: &Value, path: &[&str]) -> Option<String> {
    json_value_at(value, path)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn json_string_at_opt(value: Option<&Value>, path: &[&str]) -> Option<String> {
    value.and_then(|item| json_string_at(item, path))
}

fn json_bool_at_opt(value: Option<&Value>, path: &[&str]) -> Option<bool> {
    json_value_at_opt(value, path).and_then(Value::as_bool)
}

fn unique_traceability_links(links: Vec<TraceabilityLink>) -> Vec<TraceabilityLink> {
    let mut deduped = HashMap::new();
    for link in links {
        deduped.insert(link.link_key.clone(), link);
    }
    let mut values = deduped.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.link_key.cmp(&right.link_key))
    });
    values
}

fn tracey_status_from_probe(service: &ServiceSnapshot) -> Option<Value> {
    service
        .probe
        .get("metrics")
        .and_then(|value| value.get("status"))
        .cloned()
}

fn tracey_status_from_probe_root(service: &ServiceSnapshot) -> Option<Value> {
    service.probe.get("status").cloned()
}

fn tracey_loader_status_from_probe(service: &ServiceSnapshot) -> Option<Value> {
    service
        .probe
        .get("metrics")
        .and_then(|value| value.get("loader_status"))
        .cloned()
}

fn atlassian_sync_is_configured(config: &ConductorConfig) -> bool {
    config.integrations.atlassian.enabled
        && config
            .integrations
            .atlassian
            .base_url
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && config
            .integrations
            .atlassian
            .username
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && config
            .integrations
            .atlassian
            .api_token
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn refiner_sync_is_enabled(config: &ConductorConfig) -> bool {
    config.integrations.refiner.enabled && config.integrations.refiner.sync_interval_seconds > 0
}

fn tracey_sync_is_enabled(config: &ConductorConfig) -> bool {
    config.integrations.tracey.enabled && config.integrations.tracey.sync_interval_seconds > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ExecutionStatus, NewTraceabilityLink, NewWorkItem, RolloutStrategy, TraceabilityLink,
        WorkExecution, WorkItem, WorkStatus,
    };
    use crate::{
        config::ConductorConfig, integrations::build_http_client, storage::memory::MemoryRepository,
    };
    use axum::{
        Json, Router,
        extract::Path,
        routing::{get, post},
    };
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    async fn spawn_mock_refiner() -> (String, tokio::task::JoinHandle<()>) {
        async fn login() -> Json<Value> {
            Json(json!({"status": "ok"}))
        }

        async fn job_detail(Path(job_id): Path<String>) -> Json<Value> {
            Json(json!({
                "id": job_id,
                "workflow": "project_solver",
                "project_name": "Tracey rollout validation",
                "status": "completed",
                "progress": 100,
                "output_paths": {
                    "primary": "/tmp/refiner/jobs/job-123/project_solution.json"
                },
                "stages": [
                    {"name": "plan", "status": "completed"},
                    {"name": "apply", "status": "completed"},
                    {"name": "verify", "status": "completed"}
                ],
                "repo_info": {
                    "workspace": "/tmp/refiner/workspaces/job-123/tracey",
                    "branch": "conductor/tracey-rollout"
                },
                "workspace_env": {
                    "provider": "continuum",
                    "status": "ready",
                    "vm_id": "vm-123",
                    "ide_url": "https://ide.example/job-123",
                    "preview_url": "https://preview.example/job-123"
                },
                "payload": {
                    "project_root": "/repo/tracey",
                    "repo_url": "https://github.com/neuralmimicry/tracey",
                    "repo_branch": "main",
                    "work_branch": "conductor/tracey-rollout"
                }
            }))
        }

        async fn requirements_progress(Path(_job_id): Path<String>) -> Json<Value> {
            Json(json!({
                "total": 12,
                "completed": 12,
                "in_progress": 0,
                "remaining": 0,
                "status": "ready",
                "source": "register",
                "updated_at": "2026-04-23T12:00:00Z"
            }))
        }

        async fn requirements_summary(Path(_job_id): Path<String>) -> Json<Value> {
            Json(json!({
                "summary": "Delivery register satisfied.",
                "items": [
                    {"id": "REQ-001", "title": "Preserve canary governance"},
                    {"id": "REQ-002", "title": "Publish rollout evidence"}
                ],
                "total": 12,
                "source": "register",
                "updated_at": "2026-04-23T12:00:00Z"
            }))
        }

        async fn workspace(Path(_job_id): Path<String>) -> Json<Value> {
            Json(json!({
                "workspace": {
                    "provider": "continuum",
                    "status": "ready",
                    "vm_id": "vm-123",
                    "ide_url": "https://ide.example/job-123",
                    "preview_url": "https://preview.example/job-123"
                },
                "status": "ready"
            }))
        }

        let app = Router::new()
            .route("/api/login", post(login))
            .route(
                "/api/jobs/{job_id}/requirements/progress",
                get(requirements_progress),
            )
            .route(
                "/api/jobs/{job_id}/requirements/summary",
                get(requirements_summary),
            )
            .route("/api/jobs/{job_id}/workspace", get(workspace))
            .route("/api/jobs/{job_id}", get(job_detail));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind refiner");
        let addr = listener.local_addr().expect("refiner local addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve refiner mock");
        });
        (format!("http://{}", addr), handle)
    }

    async fn spawn_mock_tracey() -> (String, tokio::task::JoinHandle<()>) {
        async fn status() -> Json<Value> {
            Json(json!({
                "ts_ms": 1713873600000u64,
                "agent_id": "tracey-node-1",
                "agent_version": "2026.04.23",
                "status": "ok",
                "posture": "guarded",
                "continuum_loop": {
                    "mode": "hold",
                    "next_action": "hold untrusted loader promotions",
                    "overall_score": 0.64,
                    "readiness_score": 0.58,
                    "placement_score": 0.62,
                    "compromise_risk": 0.22,
                    "fuzzy_confidence": 0.71
                },
                "loader_threats": {
                    "summary": {
                        "blocked_provider_count": 1,
                        "blocked_artifact_count": 0,
                        "highest_provider_risk": 0.91,
                        "highest_artifact_risk": 0.14,
                        "remote_reporters": 2
                    }
                }
            }))
        }

        async fn loader_status() -> Json<Value> {
            Json(json!({
                "ts_ms": 1713873600000u64,
                "agent_id": "tracey-node-1",
                "version": "2026.04.23",
                "channel": "production",
                "distributable": false,
                "pending_rollback": true,
                "rollback_previous_version": "2026.04.22",
                "loader_threats": {
                    "summary": {
                        "blocked_provider_count": 1,
                        "blocked_artifact_count": 0,
                        "highest_provider_risk": 0.91,
                        "highest_artifact_risk": 0.14,
                        "remote_reporters": 2
                    }
                }
            }))
        }

        let app = Router::new()
            .route("/status", get(status))
            .route("/loader/status", get(loader_status));

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind tracey");
        let addr = listener.local_addr().expect("tracey local addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve tracey mock");
        });
        (format!("http://{}", addr), handle)
    }

    #[test]
    fn dora_metrics_use_production_stage_history() {
        let mut production_item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:gail".to_string()),
            title: "Promote Gail".to_string(),
            summary: "Advance Gail to production".to_string(),
            target_service: Some("gail".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(80),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        production_item.created_at = now_utc() - ChronoDuration::hours(48);

        let mut success = WorkExecution::new(
            production_item.id,
            Some("gail".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::Canary,
        );
        success.status = ExecutionStatus::Success;
        success.started_at = now_utc() - ChronoDuration::hours(2);
        success.updated_at = now_utc() - ChronoDuration::hours(1);
        success.finished_at = Some(now_utc() - ChronoDuration::hours(1));

        let mut failed_item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:tracey".to_string()),
            title: "Promote Tracey".to_string(),
            summary: "Advance Tracey to production".to_string(),
            target_service: Some("tracey".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::RedGreen),
            status: Some(WorkStatus::Scheduled),
            priority: Some(80),
            progress_pct: Some(80),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({}),
            depends_on: vec![],
            source: None,
            scheduled_for: None,
        });
        failed_item.created_at = now_utc() - ChronoDuration::hours(72);

        let mut failed = WorkExecution::new(
            failed_item.id,
            Some("tracey".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::RedGreen,
        );
        failed.status = ExecutionStatus::Failure;
        failed.started_at = now_utc() - ChronoDuration::hours(10);
        failed.updated_at = now_utc() - ChronoDuration::hours(9);
        failed.finished_at = Some(now_utc() - ChronoDuration::hours(9));

        let mut recovery = WorkExecution::new(
            failed_item.id,
            Some("tracey".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::Canary,
        );
        recovery.status = ExecutionStatus::Success;
        recovery.started_at = now_utc() - ChronoDuration::hours(4);
        recovery.updated_at = now_utc() - ChronoDuration::hours(3);
        recovery.finished_at = Some(now_utc() - ChronoDuration::hours(3));

        let metrics = compute_dora_metrics(
            &[production_item, failed_item],
            &[success, failed, recovery],
            &[],
            30,
        );

        assert_eq!(metrics.attempted_production_deployments, 3);
        assert_eq!(metrics.successful_production_deployments, 2);
        assert!(metrics.deployment_frequency_per_day > 0.0);
        assert!((metrics.change_failure_rate_pct - 33.33333333333333).abs() < 0.0001);
        assert!((metrics.correlated_change_failure_rate_pct - 33.33333333333333).abs() < 0.0001);
        assert_eq!(metrics.incident_linked_production_deployments, 0);
        assert_eq!(metrics.bug_linked_production_deployments, 0);
        assert_eq!(metrics.mean_time_to_restore_hours, Some(6.0));
        assert!(metrics.lead_time_hours_average.is_some());
        assert!(metrics.lead_time_hours_median.is_some());
    }

    #[test]
    fn dora_metrics_correlate_bug_and_incident_links() {
        let mut production_item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("promote:conductor".to_string()),
            title: "Promote Conductor".to_string(),
            summary: "Advance Conductor to production".to_string(),
            target_service: Some("conductor".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(90),
            progress_pct: Some(90),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec![],
            plan: json!({"finding_key": "repository_test_baseline:conductor"}),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        production_item.created_at = now_utc() - ChronoDuration::hours(24);

        let mut success = WorkExecution::new(
            production_item.id,
            Some("conductor".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::Canary,
        );
        success.status = ExecutionStatus::Success;
        success.started_at = now_utc() - ChronoDuration::hours(2);
        success.updated_at = now_utc() - ChronoDuration::hours(1);
        success.finished_at = Some(now_utc() - ChronoDuration::hours(1));

        let bug = TraceabilityLink::from_new(
            Some(production_item.id),
            NewTraceabilityLink {
                execution_id: Some(success.id),
                finding_key: Some("repository_test_baseline:conductor".to_string()),
                system: "jira".to_string(),
                reference_type: "bug".to_string(),
                reference_key: "KAN-5".to_string(),
                title: Some("Correlated production bug".to_string()),
                status: Some("To Do".to_string()),
                url: None,
                metadata: json!({}),
            },
        );
        let incident = TraceabilityLink::from_new(
            Some(production_item.id),
            NewTraceabilityLink {
                execution_id: Some(success.id),
                finding_key: Some("repository_test_baseline:conductor".to_string()),
                system: "atlassian".to_string(),
                reference_type: "incident".to_string(),
                reference_key: "INC-12".to_string(),
                title: Some("Production incident".to_string()),
                status: Some("Open".to_string()),
                url: None,
                metadata: json!({}),
            },
        );

        let metrics = compute_dora_metrics(&[production_item], &[success], &[bug, incident], 30);

        assert_eq!(metrics.attempted_production_deployments, 1);
        assert_eq!(metrics.successful_production_deployments, 1);
        assert_eq!(metrics.change_failure_rate_pct, 0.0);
        assert_eq!(metrics.correlated_change_failure_rate_pct, 100.0);
        assert_eq!(metrics.bug_linked_production_deployments, 1);
        assert_eq!(metrics.incident_linked_production_deployments, 1);
    }

    #[tokio::test]
    async fn sync_all_links_correlates_refiner_and_tracey_sources() {
        let (refiner_base_url, refiner_handle) = spawn_mock_refiner().await;
        let (tracey_base_url, tracey_handle) = spawn_mock_tracey().await;

        let mut config = ConductorConfig::default();
        config.integrations.atlassian.enabled = false;
        config.integrations.refiner.base_url = Some(refiner_base_url.clone());
        config.integrations.refiner.username = Some("admin".to_string());
        config.integrations.refiner.password = Some("secret".to_string());
        config.integrations.refiner.sync_interval_seconds = 0;
        config.integrations.tracey.base_url = Some(tracey_base_url.clone());
        config.integrations.tracey.sync_interval_seconds = 0;

        let repository = Arc::new(MemoryRepository::new());
        let client = build_http_client(2).expect("http client");
        let service = ConductorService::new(config, repository.clone(), client);

        let mut work_item = WorkItem::from_new(NewWorkItem {
            dedupe_key: Some("tracey:rollout".to_string()),
            title: "Promote Tracey with evidence".to_string(),
            summary: "Correlate rollout and runtime signals for Tracey".to_string(),
            target_service: Some("tracey".to_string()),
            delivery_stage: Some(DeliveryStage::Production),
            validated_stages: vec![
                DeliveryStage::Development,
                DeliveryStage::Testing,
                DeliveryStage::Integration,
                DeliveryStage::IntegrationTesting,
                DeliveryStage::Uat,
            ],
            rollout_strategy: Some(RolloutStrategy::Canary),
            status: Some(WorkStatus::Scheduled),
            priority: Some(95),
            progress_pct: Some(80),
            admin_override: false,
            execution_approved: true,
            verification_required: Some(true),
            tags: vec!["tracey".to_string()],
            plan: json!({"finding_key": "rollout:tracey"}),
            depends_on: vec![],
            source: Some("planner".to_string()),
            scheduled_for: None,
        });
        work_item.created_at = now_utc() - ChronoDuration::hours(36);
        repository
            .upsert_work_item(&work_item)
            .await
            .expect("insert work item");

        let mut execution = WorkExecution::new(
            work_item.id,
            Some("tracey".to_string()),
            DeliveryStage::Production,
            RolloutStrategy::Canary,
        );
        execution.refiner_job_id = Some("job-123".to_string());
        execution.status = ExecutionStatus::Success;
        execution.request_payload = json!({
            "workflow": "project_solver",
            "project_name": "Tracey rollout validation",
            "project_root": "/repo/tracey",
            "repo_url": "https://github.com/neuralmimicry/tracey",
            "repo_branch": "main",
            "work_branch": "conductor/tracey-rollout",
            "delivery_stage": "production",
            "validated_stages": ["development", "testing", "integration", "integration_testing", "uat"],
            "rollout_strategy": "canary",
            "rollout": {
                "strategy": "canary",
                "canary_percentage": 10
            }
        });
        execution.verification = json!({"passed": true});
        execution.finished_at = Some(now_utc() - ChronoDuration::minutes(30));
        execution.updated_at = now_utc() - ChronoDuration::minutes(30);
        repository
            .upsert_work_execution(&execution)
            .await
            .expect("insert execution");

        let result = service
            .sync_all_links(TraceabilitySyncRequest {
                systems: vec!["refiner".to_string(), "tracey".to_string()],
            })
            .await
            .expect("sync should succeed");

        assert!(
            result.errors.is_empty(),
            "unexpected sync errors: {:?}",
            result.errors
        );
        assert!(result.synced_systems.iter().any(|value| value == "refiner"));
        assert!(result.synced_systems.iter().any(|value| value == "tracey"));

        let job_link = result
            .links
            .iter()
            .find(|link| link.system == "refiner" && link.reference_type == "job")
            .expect("refiner job link");
        assert_eq!(job_link.reference_key, "job-123");
        assert!(
            job_link
                .url
                .as_deref()
                .is_some_and(|value| value.ends_with("/api/jobs/job-123"))
        );

        let requirements_link = result
            .links
            .iter()
            .find(|link| link.system == "refiner" && link.reference_type == "requirements")
            .expect("refiner requirements link");
        assert_eq!(
            requirements_link.metadata["progress"]["completed"],
            json!(12)
        );
        assert_eq!(requirements_link.metadata["summary"]["total"], json!(12));

        let incident_link = result
            .links
            .iter()
            .find(|link| link.system == "tracey" && link.reference_type == "incident")
            .expect("tracey incident link");
        assert_eq!(incident_link.status.as_deref(), Some("open"));
        assert_eq!(incident_link.execution_id, Some(execution.id));

        let rollback_link = result
            .links
            .iter()
            .find(|link| link.system == "tracey" && link.reference_type == "rollback")
            .expect("tracey rollback link");
        assert_eq!(rollback_link.status.as_deref(), Some("pending"));
        assert_eq!(
            rollback_link.metadata["rollback_previous_version"],
            json!("2026.04.22")
        );

        refiner_handle.abort();
        tracey_handle.abort();
    }
}
