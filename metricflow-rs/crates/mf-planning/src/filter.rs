use mf_core::manifest::WhereFilterIntersection;
use mf_manifest::graph::SemanticGraph;
use regex::Regex;
use std::sync::LazyLock;

/// A resolved filter: the SQL WHERE clause text plus the list of dimensions
/// that need to be projected in the inner SELECT for the filter to reference.
#[derive(Debug, Clone)]
pub struct ResolvedFilter {
    /// The SQL WHERE clause, e.g. "sales_channel = 'Self-Serve'"
    pub sql: String,
    /// Columns that must be projected in the inner SELECT for the WHERE to work.
    /// Each entry is (alias, expr): e.g. ("customer_arr_waterfall__sales_channel", "sales_channel")
    pub required_columns: Vec<(String, String)>,
}

// Matches {{ Dimension('entity__dim') }} or {{Dimension('entity__dim')}}
static DIMENSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\{\{\s*Dimension\(\s*'([^']+)'\s*\)\s*\}\}"#).unwrap()
});

// Matches {{ TimeDimension('entity__dim', 'grain') }} or just {{ TimeDimension('entity__dim') }}
static TIME_DIMENSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\{\{\s*TimeDimension\(\s*'([^']+)'(?:\s*,\s*'([^']+)')?\s*\)\s*\}\}"#).unwrap()
});

// Matches {{ Entity('entity_name') }}
static ENTITY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\{\{\s*Entity\(\s*'([^']+)'\s*\)\s*\}\}"#).unwrap()
});

/// Resolve a metric's where_filter_intersection into SQL WHERE clauses.
/// Replaces `{{ Dimension('entity__dim') }}` with the dimension's SQL expression,
/// and tracks which columns need to be projected for the filter to work.
pub fn resolve_filters(
    graph: &SemanticGraph,
    filter: &WhereFilterIntersection,
) -> Vec<ResolvedFilter> {
    filter
        .where_filters
        .iter()
        .map(|wf| resolve_single_filter(graph, &wf.where_sql_template))
        .collect()
}

/// Resolve a single WHERE clause template string to SQL.
/// Public wrapper for use by query-level WHERE clauses.
pub fn resolve_single_filter_public(graph: &SemanticGraph, template: &str) -> ResolvedFilter {
    resolve_single_filter(graph, template)
}

fn resolve_single_filter(graph: &SemanticGraph, template: &str) -> ResolvedFilter {
    let mut sql = template.to_string();
    let mut required_columns: Vec<(String, String)> = Vec::new();

    // Resolve {{ Dimension('entity__dim') }}
    for cap in DIMENSION_RE.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let reference = &cap[1]; // e.g., "customer_arr_waterfall__sales_channel"

        if let Some((alias, expr)) = resolve_dimension_reference(graph, reference) {
            sql = sql.replace(full_match, &alias);
            required_columns.push((alias, expr));
        } else {
            // Can't resolve — leave as-is (will likely cause a SQL error, but at least visible)
            sql = sql.replace(full_match, reference);
        }
    }

    // Resolve {{ TimeDimension('entity__dim', 'grain') }}
    for cap in TIME_DIMENSION_RE.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let reference = &cap[1];
        let grain = cap.get(2).map(|m| m.as_str()).unwrap_or("day");

        if let Some((alias, expr)) = resolve_dimension_reference(graph, reference) {
            let time_alias = format!("{alias}__{grain}");
            let time_expr = format!("DATE_TRUNC('{grain}', {expr})");
            sql = sql.replace(full_match, &time_alias);
            required_columns.push((time_alias, time_expr));
        } else {
            sql = sql.replace(full_match, reference);
        }
    }

    // Resolve {{ Entity('entity_name') }}
    for cap in ENTITY_RE.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let entity_name = &cap[1];

        if let Some(model) = graph.find_model_by_entity(entity_name) {
            // Find the entity's SQL expression
            let expr = model
                .entities
                .iter()
                .find(|e| e.name == entity_name)
                .map(|e| e.sql_expr().to_string())
                .unwrap_or_else(|| entity_name.to_string());
            sql = sql.replace(full_match, &expr);
        } else {
            sql = sql.replace(full_match, entity_name);
        }
    }

    ResolvedFilter {
        sql,
        required_columns,
    }
}

/// Resolve a dundered dimension reference like "entity__dim" to (alias, sql_expr).
/// Returns the qualified alias (entity__dim) and the dimension's SQL expression.
fn resolve_dimension_reference(
    graph: &SemanticGraph,
    reference: &str,
) -> Option<(String, String)> {
    // Split on __ to get entity path and dimension name
    // e.g., "customer_arr_waterfall__sales_channel" → entity="customer_arr_waterfall", dim="sales_channel"
    let parts: Vec<&str> = reference.splitn(2, "__").collect();
    if parts.len() != 2 {
        return None;
    }
    let entity_name = parts[0];
    let dim_name = parts[1];

    // Find the model with this primary entity
    let model = graph.find_model_by_entity(entity_name)?;

    // Find the dimension on that model
    let dim = model.dimensions.iter().find(|d| d.name == dim_name)?;

    let alias = format!("{entity_name}__{dim_name}");
    let expr = dim.sql_expr().to_string();

    Some((alias, expr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mf_core::manifest::*;
    use mf_manifest::parse;

    #[test]
    fn test_resolve_dimension_filter() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // The fixture has entity "customer" (primary) on model fct_customer_arr_waterfall
        // with dimension "plan_type" (expr="plan_type")
        let filter = WhereFilterIntersection {
            where_filters: vec![WhereFilter {
                where_sql_template:
                    "{{ Dimension('customer__plan_type') }} = 'Enterprise'".into(),
            }],
        };

        let resolved = resolve_filters(&graph, &filter);
        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved[0].sql,
            "customer__plan_type = 'Enterprise'"
        );
        assert_eq!(resolved[0].required_columns.len(), 1);
        assert_eq!(resolved[0].required_columns[0].0, "customer__plan_type");
        assert_eq!(resolved[0].required_columns[0].1, "plan_type");
    }

    #[test]
    fn test_resolve_dimension_filter_no_spaces() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // Some templates don't have spaces around braces
        let filter = WhereFilterIntersection {
            where_filters: vec![WhereFilter {
                where_sql_template:
                    "{{Dimension('customer__plan_type')}} = 'Enterprise'".into(),
            }],
        };

        let resolved = resolve_filters(&graph, &filter);
        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved[0].sql,
            "customer__plan_type = 'Enterprise'"
        );
    }

    #[test]
    fn test_resolve_entity_filter() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        let filter = WhereFilterIntersection {
            where_filters: vec![WhereFilter {
                where_sql_template: "{{ Entity('customer') }} IS NOT NULL".into(),
            }],
        };

        let resolved = resolve_filters(&graph, &filter);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].sql, "customer_id IS NOT NULL");
        assert!(resolved[0].required_columns.is_empty());
    }

    #[test]
    fn test_resolve_multiple_filters() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        let filter = WhereFilterIntersection {
            where_filters: vec![
                WhereFilter {
                    where_sql_template:
                        "{{ Dimension('customer__plan_type') }} = 'Enterprise'".into(),
                },
                WhereFilter {
                    where_sql_template: "{{ Entity('customer') }} IS NOT NULL".into(),
                },
            ],
        };

        let resolved = resolve_filters(&graph, &filter);
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].sql, "customer__plan_type = 'Enterprise'");
        assert_eq!(resolved[1].sql, "customer_id IS NOT NULL");
    }
}
