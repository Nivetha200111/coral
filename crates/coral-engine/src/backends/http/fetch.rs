//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::cache::{HttpCacheEntry, build_cache_key, estimate_json_bytes};
use crate::backends::http::client::HttpSourceClient;
use crate::backends::http::error::{pagination_error, provider_error};
use crate::backends::http::pagination::{
    PageState, apply_pagination_body_fields, apply_pagination_query_pairs, page_is_exhausted,
    pagination_state_values, resolve_page_size,
};
use crate::backends::http::request::{RequestBody, build_query_pairs, build_request_body};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::transport::{OutgoingHttpRequest, execute_request};
use crate::backends::http::url::{join_url, normalize_base_url};
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{
    RenderContext, render_template, resolve_value_source, value_to_string,
};
use coral_spec::backends::http::HttpCacheMode;
use coral_spec::{HeaderSpec, HttpMethod, ValidatedPaginationMode};

const DEFAULT_MAX_PAGES: usize = 10_000;

/// Single-flight outcome carried as `Err` for non-cacheable fetches.
/// `Clone` is required by moka; `Arc` keeps the inner error intact.
#[derive(Clone)]
enum FetchSkipped {
    NoData,
    NotCacheable {
        payload: Value,
        next_url: Option<String>,
    },
    NetworkError(Arc<DataFusionError>),
}

impl FetchSkipped {
    fn network_error(err: DataFusionError) -> Self {
        Self::NetworkError(Arc::new(err))
    }

    fn no_data() -> Self {
        Self::NoData
    }

    fn not_cacheable(payload: Value, next_url: Option<String>) -> Self {
        Self::NotCacheable { payload, next_url }
    }
}

/// Fallback wrapper exposing the inner error via `Error::source()`.
#[derive(Debug)]
struct SharedDataFusionError(Arc<DataFusionError>);

impl std::fmt::Display for SharedDataFusionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0, f)
    }
}

impl std::error::Error for SharedDataFusionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// moka keeps an internal `Arc` clone, so `try_unwrap` never succeeds;
/// downcast-clone preserves the original variant for the common case.
fn unwrap_network_error(err: Arc<DataFusionError>) -> DataFusionError {
    if let DataFusionError::External(boxed) = &*err
        && let Some(provider_err) = boxed.downcast_ref::<ProviderQueryError>()
    {
        return DataFusionError::External(Box::new(provider_err.clone()));
    }
    DataFusionError::External(Box::new(SharedDataFusionError(err)))
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "GET",
        HttpMethod::POST => "POST",
    }
}

fn hash_cache_bytes(value: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn hash_request_body(body: &RequestBody) -> u64 {
    match body {
        RequestBody::Json(value) => {
            hash_cache_bytes(serde_json::to_string(value).unwrap_or_default().as_bytes())
        }
        RequestBody::Text(text) => hash_cache_bytes(text.as_bytes()),
    }
}

fn cache_vary_header_hashes(
    request_headers: &[HeaderSpec],
    table_headers: &[HeaderSpec],
    body: Option<&RequestBody>,
    render_context: &RenderContext<'_>,
    vary_headers: &[String],
) -> Result<Vec<(String, Option<u64>)>> {
    if vary_headers.is_empty() {
        return Ok(Vec::new());
    }

    let mut header_map = HeaderMap::new();
    for header in request_headers.iter().chain(table_headers.iter()) {
        if let Some(value) = resolve_value_source(&header.value, render_context)? {
            let name = HeaderName::try_from(header.name.as_str()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "invalid request header name '{}': {error}",
                    header.name
                ))
            })?;
            let value =
                HeaderValue::try_from(value_to_string(&value).as_str()).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "invalid request header value for '{}': {error}",
                        header.name
                    ))
                })?;
            header_map.insert(name, value);
        }
    }
    if matches!(body, Some(RequestBody::Text(_)))
        && !header_map.contains_key(reqwest::header::CONTENT_TYPE)
    {
        header_map.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain"),
        );
    }

    vary_headers
        .iter()
        .map(|header| {
            let name = HeaderName::try_from(header.as_str()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "invalid cache vary header name '{header}': {error}"
                ))
            })?;
            let value_hash = header_map
                .get(&name)
                .map(|value| hash_cache_bytes(value.as_bytes()));
            Ok((name.as_str().to_string(), value_hash))
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct FetchLimits {
    effective_limit: Option<usize>,
    page_size_limit: Option<usize>,
    max_search_calls: Option<usize>,
}

#[expect(
    clippy::too_many_lines,
    reason = "Paginated fetch logic is stateful and easier to audit in one sequential function"
)]
pub(super) async fn fetch_rows(
    client: &HttpSourceClient,
    target: &HttpFetchTarget,
    filter_values: &HashMap<String, String>,
    arg_values: &HashMap<String, String>,
    sql_limit: Option<usize>,
) -> Result<Vec<Value>> {
    let mut all_rows = Vec::new();
    let limits = resolve_fetch_limits(target, sql_limit);
    let pagination = target
        .pagination()
        .validated(&client.source_schema, target.name())
        .map_err(|error| {
            provider_error(ProviderQueryError::Pagination {
                source_schema: client.source_schema.clone(),
                table: target.name().to_string(),
                method: None,
                url: None,
                detail: error.to_string(),
            })
        })?;
    let page_size = resolve_page_size(pagination.page_size.as_ref(), limits.page_size_limit);

    let active_request = target.resolved_request();

    let mut state = PageState {
        page: target.pagination().page_start,
        offset: match &pagination.mode {
            ValidatedPaginationMode::Offset(offset) => offset.start,
            _ => target.pagination().offset_start,
        },
        ..PageState::default()
    };

    let mut page_count = 0usize;
    let max_pages = target.pagination().max_pages.unwrap_or(DEFAULT_MAX_PAGES);

    loop {
        page_count += 1;
        if page_count > max_pages {
            return Err(provider_error(ProviderQueryError::Pagination {
                source_schema: client.source_schema.clone(),
                table: target.name().to_string(),
                method: None,
                url: None,
                detail: format!("exceeded pagination max_pages={max_pages}"),
            }));
        }

        let resolved_inputs = client.resolved_inputs_for_request().await?;
        let state_values = pagination_state_values(&state);
        let render_context = RenderContext::new(
            filter_values,
            arg_values,
            &state_values,
            resolved_inputs.as_ref(),
        );
        let base_url = render_template(&client.base_url, &render_context)?;
        let base_url = normalize_base_url(&base_url);
        let following_link_header = matches!(
            pagination.mode,
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto
        ) && state.next_url.is_some();

        let url = if matches!(
            pagination.mode,
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto
        ) && let Some(next) = state.next_url.clone()
        {
            next
        } else {
            let rendered_path = render_template(&active_request.path, &render_context)?;
            join_url(&base_url, &rendered_path)?
        };

        let (query_pairs, body) = if following_link_header {
            (Vec::new(), None)
        } else {
            let mut query_pairs = build_query_pairs(active_request, &render_context)?;
            apply_pagination_query_pairs(&mut query_pairs, target, &pagination, &state, page_size)
                .map_err(|error| {
                    pagination_error(
                        &client.source_schema,
                        target.name(),
                        None,
                        Some(&url),
                        &error,
                    )
                })?;

            let mut body = build_request_body(active_request, &render_context)?;
            apply_pagination_body_fields(
                &mut body,
                &active_request.body,
                target,
                &pagination,
                &state,
                page_size,
            )
            .map_err(|error| {
                pagination_error(
                    &client.source_schema,
                    target.name(),
                    None,
                    Some(&url),
                    &error,
                )
            })?;
            (query_pairs, body)
        };

        let cache_key: Option<(String, usize, Duration)> = target
            .cache()
            .filter(|p| p.mode == HttpCacheMode::Ttl)
            .filter(|policy| {
                policy
                    .max_pages
                    .is_none_or(|max_cache_pages| page_count <= max_cache_pages)
            })
            .map(|policy| {
                let body_hash = body.as_ref().map(hash_request_body);
                let vary_headers = cache_vary_header_hashes(
                    &client.request_headers,
                    &active_request.headers,
                    body.as_ref(),
                    &render_context,
                    &policy.vary_headers,
                )?;
                let key = build_cache_key(
                    &client.source_schema,
                    &client.source_version,
                    target.name(),
                    http_method_label(active_request.method),
                    &url,
                    &query_pairs,
                    body_hash,
                    &vary_headers,
                    policy.ttl.as_secs(),
                );
                let max_entry = policy.max_entry_bytes.unwrap_or(usize::MAX);
                Ok::<_, DataFusionError>((key, max_entry, policy.ttl))
            })
            .transpose()?;

        let page = if let Some((ref key, max_entry_bytes, ttl)) = cache_key {
            let source_schema = client.source_schema.clone();
            let table_name = target.name().to_string();
            let ok_path = target.response().ok_path.clone();
            let result = client
                .cache
                .try_get_or_insert_with::<_, FetchSkipped>(key, async {
                    tracing::trace!(
                        source = %source_schema,
                        table = %table_name,
                        "http cache miss"
                    );
                    let result = execute_request(
                        &client.http,
                        client.request_timeout,
                        OutgoingHttpRequest {
                            auth: &client.auth,
                            request_headers: &client.request_headers,
                            request_authenticators: &client.request_authenticators,
                            table_headers: &active_request.headers,
                            table_name: target.name(),
                            method: active_request.method,
                            base_url: &base_url,
                            url: &url,
                            query_pairs: &query_pairs,
                            body: body.as_ref(),
                            response_format: target.response().format,
                            source_schema: &client.source_schema,
                            rate_limit: &client.rate_limit,
                            body_capture: client.body_capture,
                            render_context,
                            allow_404_empty: target.response().allow_404_empty,
                            link_header_require_results: pagination.link_header_require_results,
                        },
                    )
                    .await
                    .map_err(FetchSkipped::network_error)?;
                    let Some((payload, next_url)) = result else {
                        return Err(FetchSkipped::no_data());
                    };
                    let estimated_bytes = estimate_json_bytes(&payload);
                    let ok_for_cache = ok_path.is_empty()
                        || get_path_value(&payload, &ok_path)
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                    if !ok_for_cache {
                        tracing::trace!(
                            source = %source_schema,
                            table = %table_name,
                            "http cache entry skipped: ok_path=false"
                        );
                        return Err(FetchSkipped::not_cacheable(payload, next_url));
                    }
                    if estimated_bytes > max_entry_bytes {
                        tracing::trace!(
                            source = %source_schema,
                            table = %table_name,
                            estimated_bytes,
                            "http cache entry skipped: exceeds max_entry_bytes"
                        );
                        return Err(FetchSkipped::not_cacheable(payload, next_url));
                    }
                    Ok(HttpCacheEntry {
                        payload,
                        next_url,
                        ttl,
                        estimated_bytes,
                    })
                })
                .await;

            match result {
                Ok((entry, is_fresh)) => {
                    if !is_fresh {
                        tracing::trace!(
                            source = %client.source_schema,
                            table = %target.name(),
                            "http cache hit"
                        );
                    }
                    Some((entry.payload, entry.next_url))
                }
                Err(arc) => {
                    let skipped = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());
                    match skipped {
                        FetchSkipped::NetworkError(err) => {
                            return Err(unwrap_network_error(err));
                        }
                        FetchSkipped::NoData => None,
                        FetchSkipped::NotCacheable { payload, next_url } => {
                            Some((payload, next_url))
                        }
                    }
                }
            }
        } else {
            execute_request(
                &client.http,
                client.request_timeout,
                OutgoingHttpRequest {
                    auth: &client.auth,
                    request_headers: &client.request_headers,
                    request_authenticators: &client.request_authenticators,
                    table_headers: &active_request.headers,
                    table_name: target.name(),
                    method: active_request.method,
                    base_url: &base_url,
                    url: &url,
                    query_pairs: &query_pairs,
                    body: body.as_ref(),
                    response_format: target.response().format,
                    source_schema: &client.source_schema,
                    rate_limit: &client.rate_limit,
                    body_capture: client.body_capture,
                    render_context,
                    allow_404_empty: target.response().allow_404_empty,
                    link_header_require_results: pagination.link_header_require_results,
                },
            )
            .await?
        };

        let Some((payload, next_url)) = page else {
            break;
        };

        if !target.response().ok_path.is_empty() {
            let ok = get_path_value(&payload, &target.response().ok_path)
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !ok {
                let err = if target.response().error_path.is_empty() {
                    "unknown source API error".to_string()
                } else {
                    get_path_value(&payload, &target.response().error_path)
                        .and_then(Value::as_str)
                        .unwrap_or("unknown source API error")
                        .to_string()
                };
                return Err(DataFusionError::External(Box::new(
                    ProviderQueryError::ApiRequest {
                        source_schema: client.source_schema.clone(),
                        table: target.name().to_string(),
                        status: None,
                        method: None,
                        url: None,
                        filters: filter_values.clone(),
                        detail: err,
                    },
                )));
            }
        }

        let mut rows = extract_rows(target.response(), &payload);
        let rows_on_page = rows.len();
        all_rows.append(&mut rows);

        if let Some(limit) = limits.effective_limit
            && all_rows.len() >= limit
        {
            all_rows.truncate(limit);
            break;
        }

        if limits
            .max_search_calls
            .is_some_and(|max_calls| page_count >= max_calls)
        {
            break;
        }

        match &pagination.mode {
            ValidatedPaginationMode::None => break,
            ValidatedPaginationMode::CursorQuery | ValidatedPaginationMode::CursorBody => {
                let next_cursor =
                    get_path_value(&payload, &target.pagination().response_cursor_path)
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(ToOwned::to_owned);
                match next_cursor {
                    Some(cursor) => state.cursor = Some(cursor),
                    None => break,
                }
            }
            ValidatedPaginationMode::Page => {
                if page_is_exhausted(rows_on_page, page_size) {
                    break;
                }
                state.page = state.page.saturating_add(target.pagination().page_step);
            }
            ValidatedPaginationMode::Offset(offset) => {
                if page_is_exhausted(rows_on_page, page_size) {
                    break;
                }
                let step = offset
                    .resolve_step(page_size, &client.source_schema, target.name())
                    .map_err(|error| {
                        provider_error(ProviderQueryError::Pagination {
                            source_schema: client.source_schema.clone(),
                            table: target.name().to_string(),
                            method: None,
                            url: None,
                            detail: error.to_string(),
                        })
                    })?;
                state.offset = state.offset.saturating_add(step);
            }
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto => match next_url {
                Some(next) => state.next_url = Some(next),
                None => break,
            },
        }
    }

    Ok(all_rows)
}

fn resolve_fetch_limits(target: &HttpFetchTarget, sql_limit: Option<usize>) -> FetchLimits {
    let Some(search_limits) = target.search_limits() else {
        return FetchLimits {
            effective_limit: sql_limit.or(target.fetch_limit_default()),
            page_size_limit: sql_limit,
            max_search_calls: None,
        };
    };

    let requested_top_k = sql_limit.unwrap_or(search_limits.default_top_k);
    let max_candidates = search_limits
        .max_top_k
        .saturating_mul(search_limits.max_calls_per_query);

    FetchLimits {
        effective_limit: Some(requested_top_k.min(max_candidates)),
        page_size_limit: Some(requested_top_k.min(search_limits.max_top_k)),
        max_search_calls: Some(search_limits.max_calls_per_query),
    }
}
