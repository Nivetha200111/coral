//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use coral_engine::{
    CatalogInfo, CoralQuery, CoreError, QueryExecution, QueryPlan, QueryRuntimeConfig,
    QueryRuntimeContext, QuerySource, SourceValidationReport, StatisticsProfile, StatusCode,
    TableInfo,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use opentelemetry::{KeyValue, trace::Status as OtelStatus};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::query::extensions::{EngineExtensionsProvider, engine_extensions_for_providers};
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::model::InstalledSource;
use crate::state::{AppStateLayout, ConfigStore, StatisticsStore};
use crate::telemetry::InstalledLocalTraceStore;
use crate::workspaces::WorkspaceName;

#[derive(Debug)]
pub(crate) enum QueryManagerError {
    App(AppError),
    Core(CoreError),
}

pub(crate) struct ValidatedSource {
    pub(crate) source: InstalledSource,
    pub(crate) report: SourceValidationReport,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    statistics_store: StatisticsStore,
    local_trace_store: Option<InstalledLocalTraceStore>,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self {
            config_store,
            credential_manager,
            statistics_store: StatisticsStore::new(layout.clone()),
            local_trace_store: None,
            runtime_context,
            layout,
            engine_extensions_providers,
        }
    }

    pub(crate) fn with_local_trace_store(
        mut self,
        local_trace_store: Option<InstalledLocalTraceStore>,
    ) -> Self {
        self.local_trace_store = local_trace_store;
        self
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_config(&sources, StatisticsProfile::empty());
        CoralQuery::list_tables(&sources, runtime, schema_filter, table_filter)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_config(&sources, StatisticsProfile::empty());
        CoralQuery::list_catalog(&sources, runtime, schema_filter)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        let result = run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            async {
                let sources = self
                    .load_query_sources(workspace_name)
                    .map_err(QueryManagerError::App)?;
                let statistics = self.load_statistics_profile(workspace_name, &sources);
                let runtime = self.runtime_config(&sources, statistics);
                let execution = CoralQuery::execute_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)?;
                Ok(execution)
            },
            |execution| Some(u64::try_from(execution.row_count()).unwrap_or(u64::MAX)),
        )
        .await;
        if result.is_ok() {
            self.refresh_statistics_profile_from_traces(workspace_name)
                .await;
        }
        result
    }

    pub(crate) async fn explain_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryPlan, QueryManagerError> {
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            async {
                let sources = self
                    .load_query_sources(workspace_name)
                    .map_err(QueryManagerError::App)?;
                let statistics = self.load_statistics_profile(workspace_name, &sources);
                let runtime = self.runtime_config(&sources, statistics);
                CoralQuery::explain_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |_| None,
        )
        .await
    }

    pub(crate) async fn validate_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<ValidatedSource, QueryManagerError> {
        let source = self
            .config_store
            .get_source(workspace_name, source_name)
            .map_err(QueryManagerError::App)?;
        let (query_source, version) = self
            .load_query_source(workspace_name, &source)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_config(
            std::slice::from_ref(&query_source),
            StatisticsProfile::empty(),
        );
        let report = CoralQuery::validate_source(
            &query_source,
            runtime,
            query_source.source_spec().test_queries(),
        )
        .await
        .map_err(QueryManagerError::Core)?;
        let mut source = source;
        source.version = Some(version);

        Ok(ValidatedSource { source, report })
    }

    fn load_query_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<QuerySource>, AppError> {
        let catalog = self.config_store.load_catalog()?;
        let mut query_sources = Vec::new();
        for source in catalog.workspace_sources(workspace_name) {
            match self.load_query_source(workspace_name, &source) {
                Ok((query_source, _version)) => query_sources.push(query_source),
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "skipping source during query-source load"
                    );
                }
            }
        }
        Ok(query_sources)
    }

    fn load_query_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(QuerySource, String), AppError> {
        let installed = resolve_installed_manifest(workspace_name, source, &self.layout)?;
        let source_spec = installed.source_spec;
        validate_required_variables(source, source_spec.declared_inputs())?;
        let credential_set_id = CredentialSetId::for_source(&source.name);
        let stored_secrets = self
            .credential_manager
            .read_material(workspace_name, &credential_set_id)?;
        let mut resolved_secrets = BTreeMap::new();
        let missing_secrets: Vec<String> = source_spec
            .required_secret_names()
            .into_iter()
            .filter(|name| !stored_secrets.contains_key(name))
            .collect();
        if let Some((first, rest)) = missing_secrets.split_first() {
            let detail = if rest.is_empty() {
                format!("secret '{first}'")
            } else {
                format!("secret '{first}' and {} other(s)", rest.len())
            };
            return Err(AppError::FailedPrecondition(format!(
                "source '{}' is missing {detail}",
                source.name
            )));
        }
        for secret_name in source_spec.required_secret_names() {
            let value = stored_secrets.get(&secret_name).cloned().ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "source '{}' is missing secret '{secret_name}'",
                    source.name
                ))
            })?;
            resolved_secrets.insert(secret_name, value);
        }
        Ok((
            QuerySource::new(source_spec, source.variables.clone(), resolved_secrets),
            installed.candidate.version,
        ))
    }

    fn load_statistics_profile(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> StatisticsProfile {
        match self.statistics_store.load_profile(workspace_name) {
            Ok(mut profile) => {
                profile.retain_sources(selected_sources.iter().map(QuerySource::source_name));
                profile
            }
            Err(error) => {
                tracing::warn!(
                    workspace = %workspace_name,
                    detail = %error,
                    "failed to load workspace statistics profile"
                );
                StatisticsProfile::empty()
            }
        }
    }

    fn runtime_config(
        &self,
        selected_sources: &[QuerySource],
        statistics: StatisticsProfile,
    ) -> QueryRuntimeConfig {
        QueryRuntimeConfig::new(
            self.runtime_context.clone(),
            engine_extensions_for_providers(&self.engine_extensions_providers, selected_sources),
        )
        .with_statistics(statistics)
    }

    async fn refresh_statistics_profile_from_traces(&self, workspace_name: &WorkspaceName) {
        let Some(local_trace_store) = &self.local_trace_store else {
            return;
        };

        crate::telemetry::force_flush_tracing();

        let observations =
            match crate::telemetry::load_statistics_observations(local_trace_store).await {
                Ok(observations) => observations,
                Err(error) => {
                    tracing::warn!(
                        workspace = %workspace_name,
                        detail = %error,
                        "failed to load statistics observations from local trace history"
                    );
                    return;
                }
            };
        let sources = match self.load_query_sources(workspace_name) {
            Ok(sources) => sources,
            Err(error) => {
                tracing::warn!(
                    workspace = %workspace_name,
                    detail = %error,
                    "failed to refresh query sources for statistics profile materialization"
                );
                return;
            }
        };
        if let Err(error) = self.statistics_store.rebuild_profile_from_observations(
            workspace_name,
            &observations,
            sources
                .iter()
                .map(|source| source.source_name().to_string()),
        ) {
            tracing::warn!(
                workspace = %workspace_name,
                detail = %error,
                "failed to rebuild statistics profile from local trace history"
            );
        }
    }
}

#[derive(Clone, Copy)]
enum QueryOperation {
    ExecuteSql,
    ExplainSql,
}

impl QueryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ExplainSql => "explain_sql",
        }
    }
}

async fn run_query_operation<T, Fut, RowCount>(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    query: Fut,
    row_count: RowCount,
) -> Result<T, QueryManagerError>
where
    Fut: Future<Output = Result<T, QueryManagerError>>,
    RowCount: FnOnce(&T) -> Option<u64>,
{
    let started_at = Instant::now();
    let query_span = create_query_span(operation, workspace_name, sql);
    let result = query.instrument(query_span.clone()).await;

    let metrics = crate::telemetry::metrics::metrics();
    let status = crate::telemetry::metrics::status_attr(result.is_ok());
    let attributes = [status, KeyValue::new("operation", operation.as_str())];
    metrics.count.add(1, &attributes);
    metrics
        .duration
        .record(started_at.elapsed().as_secs_f64(), &attributes);

    if let Ok(value) = &result {
        query_span.record("status", "ok");
        query_span.set_status(OtelStatus::Ok);
        if let Some(row_count) = row_count(value) {
            query_span.record("row_count", row_count);
            metrics.rows.record(row_count, &attributes);
        }
    } else if let Err(error) = &result {
        let error_kind = query_error_kind(error);
        let error_type = query_error_type(error);
        let error_message = query_error_message(error);
        query_span.record("status", "error");
        query_span.record("error.kind", error_kind);
        query_span.record("error.type", error_type.as_str());
        query_span.record("exception.message", error_message.as_str());
        query_span.set_status(OtelStatus::error(error_message));
    }

    result
}

fn create_query_span(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
) -> tracing::Span {
    let operation = operation.as_str();
    tracing::info_span!(
        "coral.query",
        otel.name = "coral.query",
        operation = operation,
        workspace = %workspace_name.as_str(),
        sql = %sql,
        row_count = tracing::field::Empty,
        status = tracing::field::Empty,
        error.kind = tracing::field::Empty,
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    )
}

fn query_error_kind(error: &QueryManagerError) -> &'static str {
    match error {
        QueryManagerError::App(_) => "app",
        QueryManagerError::Core(_) => "core",
    }
}

fn query_error_type(error: &QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => app_error_type(error).to_string(),
        QueryManagerError::Core(error) => core_error_type(error),
    }
}

fn query_error_message(error: &QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => error.to_string(),
        QueryManagerError::Core(CoreError::QueryFailure(error)) => error.summary().to_string(),
        QueryManagerError::Core(error) => error.to_string(),
    }
}

fn app_error_type(error: &AppError) -> &'static str {
    match error {
        AppError::SourceNotFound(_) => "SOURCE_NOT_FOUND",
        AppError::InvalidInput(_) => "INVALID_INPUT",
        AppError::FailedPrecondition(_) => "FAILED_PRECONDITION",
        AppError::Io(_) => "IO",
        AppError::Yaml(_) => "YAML",
        AppError::TomlDecode(_) => "TOML_DECODE",
        AppError::TomlEncode(_) => "TOML_ENCODE",
        AppError::Json(_) => "JSON",
        AppError::Transport(_) => "TRANSPORT",
        AppError::TaskJoin(_) => "TASK_JOIN",
        AppError::Credentials(_) => "CREDENTIALS",
        AppError::MissingConfigDir => "MISSING_CONFIG_DIR",
    }
}

fn core_error_type(error: &CoreError) -> String {
    match error {
        CoreError::QueryFailure(error) => error.reason().to_string(),
        error => status_code_error_type(error.status_code()).to_string(),
    }
}

fn status_code_error_type(status: StatusCode) -> &'static str {
    match status {
        StatusCode::InvalidArgument => "INVALID_ARGUMENT",
        StatusCode::NotFound => "NOT_FOUND",
        StatusCode::FailedPrecondition => "FAILED_PRECONDITION",
        StatusCode::Unavailable => "UNAVAILABLE",
        StatusCode::Unimplemented => "UNIMPLEMENTED",
        StatusCode::Internal => "INTERNAL",
    }
}

fn validate_required_variables(
    source: &InstalledSource,
    inputs: &[ManifestInputSpec],
) -> Result<(), AppError> {
    let missing: Vec<_> = inputs
        .iter()
        .filter(|input| {
            input.kind == ManifestInputKind::Variable
                && input.required
                && !source.variables.contains_key(&input.key)
        })
        .collect();
    if let Some((first, rest)) = missing.split_first() {
        let detail = if rest.is_empty() {
            format!("variable '{}'", first.key)
        } else {
            format!("variable '{}' and {} other(s)", first.key, rest.len())
        };
        return Err(AppError::FailedPrecondition(format!(
            "source '{}' is missing {detail}",
            source.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::json::ArrayWriter;
    use coral_engine::{QueryExecution, QueryRuntimeContext, StatisticsProfile};
    use serde_json::Value;
    use tempfile::{TempDir, tempdir};

    use super::QueryManager;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::query::extensions::EngineExtensionsProvider;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore, StatisticsStore};
    use crate::storage::fs;
    use crate::workspaces::WorkspaceName;

    struct QueryManagerFixture {
        _temp: TempDir,
        manager: QueryManager,
    }

    fn query_manager_with(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> QueryManagerFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            runtime_context,
            layout,
            providers,
        );
        QueryManagerFixture {
            _temp: temp,
            manager,
        }
    }

    #[test]
    fn runtime_config_preserves_app_owned_http_body_capture_max_bytes() {
        let fixture = query_manager_with(
            QueryRuntimeContext::default().with_http_body_capture_max_bytes(Some(42)),
            Vec::new(),
        );

        let runtime = fixture
            .manager
            .runtime_config(&[], StatisticsProfile::empty());

        let config = runtime
            .context
            .http_body_capture_max_bytes
            .expect("body capture config");
        assert_eq!(config, 42);
    }

    #[tokio::test]
    async fn execute_sql_without_trace_history_keeps_statistics_empty() {
        let temp = tempdir().expect("tempdir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("layout should be created");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("local_stats_jsonl").expect("source");
        let data_dir = temp.path().join("jsonl-data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        std::fs::write(
            data_dir.join("events.jsonl"),
            r#"{"id":1,"category":"alpha","nullable_text":"one"}
{"id":2,"category":"beta","nullable_text":null}
{"id":3,"category":"alpha","nullable_text":"three"}
"#,
        )
        .expect("jsonl fixture");

        let manifest = format!(
            r#"name: local_stats_jsonl
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Events
    format: jsonl
    source:
      location: file://{}/
      glob: "**/*.jsonl"
    columns:
      - name: id
        type: Int64
      - name: category
        type: Utf8
      - name: nullable_text
        type: Utf8
        nullable: true
"#,
            data_dir.display()
        );
        let manifest_path = layout.manifest_file(&workspace, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("manifest dir");
        fs::write_atomic(&manifest_path, manifest.as_bytes()).expect("manifest write");

        let config_store = ConfigStore::new(layout.clone());
        config_store
            .upsert_source(
                &workspace,
                InstalledSource {
                    name: source_name,
                    version: Some("0.1.0".to_string()),
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("install source");
        let manager = QueryManager::new(
            config_store,
            CredentialManager::new(CredentialStore::new(layout.clone())),
            QueryRuntimeContext::default(),
            layout.clone(),
            Vec::<Arc<dyn crate::query::extensions::EngineExtensionsProvider>>::new(),
        );

        manager
            .execute_sql(
                &workspace,
                "SELECT id, category, nullable_text FROM local_stats_jsonl.events ORDER BY id",
            )
            .await
            .expect("query should succeed");

        let stored = StatisticsStore::new(layout.clone())
            .load_profile(&workspace)
            .expect("profile should load");
        assert!(stored.sources.is_empty());

        let catalog = manager
            .execute_sql(
                &workspace,
                "SELECT column_name, stats_sample_count, approx_distinct_count, stats_precision \
                 FROM coral.columns \
                 WHERE schema_name = 'local_stats_jsonl' AND table_name = 'events' \
                 ORDER BY ordinal_position",
            )
            .await
            .expect("catalog query should succeed");
        let rows = execution_rows(&catalog);
        let by_name = rows
            .iter()
            .map(|row| (row["column_name"].as_str().expect("column name"), row))
            .collect::<std::collections::HashMap<_, _>>();

        let category = by_name.get("category").expect("category row");
        assert!(
            category
                .get("stats_sample_count")
                .is_none_or(Value::is_null)
        );
        assert!(
            category
                .get("approx_distinct_count")
                .is_none_or(Value::is_null)
        );
        assert!(category.get("stats_precision").is_none_or(Value::is_null));
    }

    #[tokio::test]
    async fn failed_query_does_not_create_statistics_profile() {
        let temp = tempdir().expect("tempdir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("layout should be created");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            QueryRuntimeContext::default(),
            layout.clone(),
            Vec::<Arc<dyn crate::query::extensions::EngineExtensionsProvider>>::new(),
        );

        let result = manager
            .execute_sql(&workspace, "SELECT * FROM missing.table")
            .await;

        let _ = result.unwrap_err();
        let profile = StatisticsStore::new(layout)
            .load_profile(&workspace)
            .expect("profile should load");
        assert!(profile.sources.is_empty());
    }

    #[tokio::test]
    async fn list_metadata_does_not_load_persisted_statistics_profile() {
        let temp = tempdir().expect("tempdir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("layout should be created");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let profile_path = layout.statistics_profile_file(&workspace);
        std::fs::create_dir_all(profile_path.parent().expect("profile parent"))
            .expect("profile dir");
        std::fs::write(&profile_path, b"{ malformed json").expect("profile write");
        let manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            QueryRuntimeContext::default(),
            layout,
            Vec::<Arc<dyn crate::query::extensions::EngineExtensionsProvider>>::new(),
        );

        let tables = manager
            .list_tables(&workspace, None, None)
            .await
            .expect("list tables should not read malformed stats profile");
        let catalog = manager
            .list_catalog(&workspace, None)
            .await
            .expect("list catalog should not read malformed stats profile");

        assert!(tables.is_empty());
        assert!(catalog.tables.is_empty());
        assert!(catalog.table_functions.is_empty());
    }

    fn execution_rows(execution: &QueryExecution) -> Vec<Value> {
        let mut bytes = Vec::new();
        {
            let mut writer = ArrayWriter::new(&mut bytes);
            for batch in execution.batches() {
                writer.write(batch).expect("batch should encode");
            }
            writer.finish().expect("writer should finish");
        }
        serde_json::from_slice(&bytes).expect("json rows")
    }
}
