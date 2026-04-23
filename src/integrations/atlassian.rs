use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::config::AtlassianConfig;

#[derive(Clone)]
pub struct AtlassianClients {
    pub jira: JiraClient,
    pub confluence: ConfluenceClient,
}

impl AtlassianClients {
    pub fn from_config(config: &AtlassianConfig) -> Result<Self> {
        if !config.enabled {
            return Err(anyhow!("atlassian integration is disabled"));
        }
        let base_url = config
            .base_url
            .clone()
            .ok_or_else(|| anyhow!("atlassian base_url is not configured"))?;
        let username = config
            .username
            .clone()
            .ok_or_else(|| anyhow!("atlassian username is not configured"))?;
        let api_token = config
            .api_token
            .clone()
            .ok_or_else(|| anyhow!("atlassian api_token is not configured"))?;
        let client = super::build_http_client(config.timeout_seconds.max(1))?;
        Ok(Self {
            jira: JiraClient::new(
                client.clone(),
                base_url.clone(),
                username.clone(),
                api_token.clone(),
            ),
            confluence: ConfluenceClient::new(client, base_url, username, api_token),
        })
    }
}

#[derive(Clone)]
pub struct JiraClient {
    client: Client,
    base_url: String,
    username: String,
    api_token: String,
}

#[derive(Clone)]
pub struct ConfluenceClient {
    client: Client,
    base_url: String,
    username: String,
    api_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JiraIssueSummary {
    pub issue_key: String,
    pub issue_id: String,
    pub project_key: String,
    pub summary: String,
    pub issue_type: String,
    pub status: String,
    pub url: String,
    pub labels: Vec<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfluencePageSummary {
    pub page_id: String,
    pub title: String,
    pub space_key: String,
    pub version_number: i64,
    pub labels: Vec<String>,
    pub updated_at: Option<DateTime<Utc>>,
    pub url: String,
}

impl JiraClient {
    fn new(client: Client, base_url: String, username: String, api_token: String) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            username,
            api_token,
        }
    }

    pub async fn search_issues(&self, jql: &str, limit: usize) -> Result<Vec<JiraIssueSummary>> {
        let data = self
            .get_json(
                "/rest/api/3/search/jql",
                vec![
                    ("jql", jql.to_string()),
                    ("maxResults", limit.clamp(1, 100).to_string()),
                    (
                        "fields",
                        "summary,issuetype,status,labels,updated,project".to_string(),
                    ),
                ],
            )
            .await?;
        Ok(data
            .get("issues")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|issue| map_jira_issue(&self.base_url, &issue))
            .collect())
    }

    pub async fn get_issue(&self, issue_key: &str) -> Result<JiraIssueSummary> {
        let issue_key = issue_key.trim();
        if issue_key.is_empty() {
            return Err(anyhow!("jira issue key must not be empty"));
        }
        let data = self
            .get_json(
                &format!("/rest/api/3/issue/{}", issue_key),
                vec![(
                    "fields",
                    "summary,issuetype,status,labels,updated,project".to_string(),
                )],
            )
            .await?;
        Ok(map_jira_issue(&self.base_url, &data))
    }

    pub async fn create_issue(
        &self,
        project_key: &str,
        summary: &str,
        issue_type: &str,
        description: Option<&str>,
        labels: &[String],
        fields: &Value,
    ) -> Result<JiraIssueSummary> {
        let mut payload_fields = fields_object(fields);
        payload_fields
            .entry("project".to_string())
            .or_insert_with(|| json!({"key": project_key.trim()}));
        payload_fields
            .entry("summary".to_string())
            .or_insert_with(|| Value::String(summary.trim().to_string()));
        payload_fields
            .entry("issuetype".to_string())
            .or_insert_with(|| json!({"name": issue_type.trim()}));
        if let Some(description) = description.map(str::trim).filter(|value| !value.is_empty()) {
            payload_fields
                .entry("description".to_string())
                .or_insert_with(|| Value::String(description.to_string()));
        }
        if !labels.is_empty() {
            payload_fields
                .entry("labels".to_string())
                .or_insert_with(|| json!(labels));
        }
        let created = self
            .post_json("/rest/api/2/issue", &json!({"fields": payload_fields}))
            .await?;
        let issue_key = created
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("jira create did not return an issue key"))?;
        self.get_issue(issue_key).await
    }

    pub async fn update_issue(
        &self,
        issue_key: &str,
        summary: Option<&str>,
        description: Option<&str>,
        labels: Option<&[String]>,
        fields: &Value,
    ) -> Result<JiraIssueSummary> {
        let mut payload_fields = fields_object(fields);
        if let Some(summary) = summary.map(str::trim).filter(|value| !value.is_empty()) {
            payload_fields.insert("summary".to_string(), Value::String(summary.to_string()));
        }
        if let Some(description) = description.map(str::trim).filter(|value| !value.is_empty()) {
            payload_fields.insert(
                "description".to_string(),
                Value::String(description.to_string()),
            );
        }
        if let Some(labels) = labels.filter(|value| !value.is_empty()) {
            payload_fields.insert("labels".to_string(), json!(labels));
        }
        self.put_json(
            &format!("/rest/api/2/issue/{}", issue_key.trim()),
            &json!({"fields": payload_fields}),
        )
        .await?;
        self.get_issue(issue_key).await
    }

    pub async fn transition_issue(&self, issue_key: &str, transition_name: &str) -> Result<()> {
        let issue_key = issue_key.trim();
        let transition_name = transition_name.trim();
        if issue_key.is_empty() || transition_name.is_empty() {
            return Err(anyhow!(
                "jira issue key and transition_name must not be empty"
            ));
        }
        let transitions = self
            .get_json(
                &format!("/rest/api/2/issue/{}/transitions", issue_key),
                Vec::new(),
            )
            .await?;
        let transition_id = transitions
            .get("transitions")
            .and_then(Value::as_array)
            .and_then(|items| {
                items.iter().find_map(|item| {
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .unwrap_or_default();
                    if name.eq_ignore_ascii_case(transition_name) {
                        item.get("id")
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| anyhow!("jira transition '{}' not found", transition_name))?;
        self.post_json(
            &format!("/rest/api/2/issue/{}/transitions", issue_key),
            &json!({"transition": {"id": transition_id}}),
        )
        .await?;
        Ok(())
    }

    async fn get_json(&self, path: &str, params: Vec<(&str, String)>) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(url)
            .basic_auth(&self.username, Some(&self.api_token))
            .query(&params)
            .send()
            .await?;
        super::decode_json(response).await
    }

    async fn post_json(&self, path: &str, payload: &Value) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .post(url)
            .basic_auth(&self.username, Some(&self.api_token))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Atlassian-Token", "no-check")
            .json(payload)
            .send()
            .await?;
        super::decode_json(response).await
    }

    async fn put_json(&self, path: &str, payload: &Value) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .put(url)
            .basic_auth(&self.username, Some(&self.api_token))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Atlassian-Token", "no-check")
            .json(payload)
            .send()
            .await?;
        super::decode_json(response).await
    }
}

impl ConfluenceClient {
    fn new(client: Client, base_url: String, username: String, api_token: String) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            username,
            api_token,
        }
    }

    pub async fn find_page_by_title(
        &self,
        space_key: &str,
        title: &str,
    ) -> Result<Option<ConfluencePageSummary>> {
        let data = self
            .get_json(
                "/rest/api/content",
                vec![
                    ("spaceKey", space_key.trim().to_string()),
                    ("title", title.trim().to_string()),
                    ("type", "page".to_string()),
                    ("limit", "1".to_string()),
                    ("expand", "version,space,metadata.labels".to_string()),
                ],
            )
            .await?;
        Ok(data
            .get("results")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .map(|item| map_confluence_page(&self.base_url, item)))
    }

    pub async fn get_page(&self, page_id: &str) -> Result<ConfluencePageSummary> {
        let data = self
            .get_json(
                &format!("/rest/api/content/{}", page_id.trim()),
                vec![("expand", "version,space,metadata.labels".to_string())],
            )
            .await?;
        Ok(map_confluence_page(&self.base_url, &data))
    }

    pub async fn create_page(
        &self,
        space_key: &str,
        title: &str,
        body_storage: &str,
        parent_page_id: Option<&str>,
    ) -> Result<ConfluencePageSummary> {
        let mut payload = json!({
            "type": "page",
            "title": title.trim(),
            "space": {"key": space_key.trim()},
            "body": {"storage": {"value": body_storage, "representation": "storage"}},
        });
        if let Some(parent_page_id) = parent_page_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            payload["ancestors"] = json!([{"id": parent_page_id}]);
        }
        let created = self.post_json("/rest/api/content", &payload).await?;
        let page_id = created
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("confluence create did not return a page id"))?;
        self.get_page(page_id).await
    }

    pub async fn update_page(
        &self,
        page_id: &str,
        title: Option<&str>,
        body_storage: &str,
        parent_page_id: Option<&str>,
    ) -> Result<ConfluencePageSummary> {
        let current = self.get_page(page_id).await?;
        let mut payload = json!({
            "id": current.page_id,
            "type": "page",
            "title": title
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(current.title.as_str()),
            "version": {"number": current.version_number + 1},
            "space": {"key": current.space_key},
            "body": {"storage": {"value": body_storage, "representation": "storage"}},
        });
        if let Some(parent_page_id) = parent_page_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            payload["ancestors"] = json!([{"id": parent_page_id}]);
        }
        self.put_json(&format!("/rest/api/content/{}", page_id.trim()), &payload)
            .await?;
        self.get_page(page_id).await
    }

    pub async fn add_labels(&self, page_id: &str, labels: &[String]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }
        self.post_json(
            &format!("/rest/api/content/{}/label", page_id.trim()),
            &json!(
                labels
                    .iter()
                    .map(|label| json!({"prefix": "global", "name": label}))
                    .collect::<Vec<_>>()
            ),
        )
        .await?;
        Ok(())
    }

    async fn get_json(&self, path: &str, params: Vec<(&str, String)>) -> Result<Value> {
        let url = format!("{}{}", confluence_base_url(&self.base_url), path);
        let response = self
            .client
            .get(url)
            .basic_auth(&self.username, Some(&self.api_token))
            .query(&params)
            .send()
            .await?;
        super::decode_json(response).await
    }

    async fn post_json(&self, path: &str, payload: &Value) -> Result<Value> {
        let url = format!("{}{}", confluence_base_url(&self.base_url), path);
        let response = self
            .client
            .post(url)
            .basic_auth(&self.username, Some(&self.api_token))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Atlassian-Token", "no-check")
            .json(payload)
            .send()
            .await?;
        super::decode_json(response).await
    }

    async fn put_json(&self, path: &str, payload: &Value) -> Result<Value> {
        let url = format!("{}{}", confluence_base_url(&self.base_url), path);
        let response = self
            .client
            .put(url)
            .basic_auth(&self.username, Some(&self.api_token))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Atlassian-Token", "no-check")
            .json(payload)
            .send()
            .await?;
        super::decode_json(response).await
    }
}

fn fields_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn map_jira_issue(base_url: &str, raw: &Value) -> JiraIssueSummary {
    let fields = raw.get("fields").cloned().unwrap_or_else(|| json!({}));
    let issue_key = raw
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    JiraIssueSummary {
        issue_key: issue_key.clone(),
        issue_id: raw
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        project_key: fields
            .get("project")
            .and_then(|value| value.get("key"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        summary: fields
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        issue_type: fields
            .get("issuetype")
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        status: fields
            .get("status")
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        url: if issue_key.is_empty() {
            String::new()
        } else {
            format!("{}/browse/{}", base_url.trim_end_matches('/'), issue_key)
        },
        labels: fields
            .get("labels")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        updated_at: parse_atlassian_datetime(fields.get("updated").and_then(Value::as_str)),
    }
}

fn map_confluence_page(base_url: &str, raw: &Value) -> ConfluencePageSummary {
    let page_id = raw
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let space_key = raw
        .get("space")
        .and_then(|value| value.get("key"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    ConfluencePageSummary {
        page_id: page_id.clone(),
        title: raw
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        space_key,
        version_number: raw
            .get("version")
            .and_then(|value| value.get("number"))
            .and_then(Value::as_i64)
            .unwrap_or(1),
        labels: raw
            .get("metadata")
            .and_then(|value| value.get("labels"))
            .and_then(|value| value.get("results"))
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.get("name").and_then(Value::as_str))
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        updated_at: parse_atlassian_datetime(
            raw.get("version")
                .and_then(|value| value.get("when"))
                .and_then(Value::as_str),
        ),
        url: if page_id.is_empty() {
            String::new()
        } else {
            format!("{}/pages/{}", confluence_base_url(base_url), page_id)
        },
    }
}

fn confluence_base_url(base_url: &str) -> String {
    let base_url = base_url.trim_end_matches('/');
    if base_url.ends_with("/wiki") {
        base_url.to_string()
    } else {
        format!("{}/wiki", base_url)
    }
}

fn parse_atlassian_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let normalized = value.replace('Z', "+00:00");
    DateTime::parse_from_rfc3339(&normalized)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.3f%z")
                .map(|value| value.with_timezone(&Utc))
        })
        .or_else(|_| {
            DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%z")
                .map(|value| value.with_timezone(&Utc))
        })
        .ok()
}
