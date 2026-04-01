use crate::dataflow::*;
use crate::resolve::{self, ResolveError};
use mf_core::spec::*;
use mf_manifest::graph::SemanticGraph;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error("only simple metrics are supported in this version")]
    UnsupportedMetricType,
}

/// Build a dataflow plan for the given query.
/// Phase 1-2: only supports simple metrics with dimensions on the same semantic model.
pub fn build_plan(graph: &SemanticGraph, query: &QuerySpec) -> Result<DataflowPlan, PlanError> {
    // For now: support exactly one metric
    // (Multi-metric queries are a later phase)
    if query.metrics.len() != 1 {
        return Err(PlanError::UnsupportedMetricType);
    }

    let metric_name = &query.metrics[0];
    let resolved = resolve::resolve_simple_metric(graph, metric_name)?;

    let mut plan = DataflowPlan::new();

    // Step 1: ReadFromSource
    let read_node = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    // Step 2: Build group-by column names and aggregation
    let group_by_columns: Vec<String> = query.group_by.iter().map(|g| g.column_name()).collect();

    let aggregations = vec![MeasureAggregation {
        measure_name: resolved.measure.name.clone(),
        agg_type: resolved.measure.agg,
        expr: resolved.measure.sql_expr().to_string(),
        alias: resolved.metric.name.clone(),
    }];

    // Step 3: Aggregate node
    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations,
    });
    plan.add_edge(read_node, agg_node);

    // Step 4: Optional ORDER BY
    let mut current = agg_node;
    if !query.order_by.is_empty() {
        let order_specs: Vec<(String, bool)> = query
            .order_by
            .iter()
            .map(|o| (o.column.column_name(), o.descending))
            .collect();
        let order_node = plan.add_node(DataflowNode::OrderBy { specs: order_specs });
        plan.add_edge(current, order_node);
        current = order_node;
    }

    // Step 5: Optional LIMIT
    if let Some(count) = query.limit {
        let limit_node = plan.add_node(DataflowNode::Limit { count });
        plan.add_edge(current, limit_node);
        current = limit_node;
    }

    plan.set_sink(current);
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::DataflowNode;
    use mf_core::types::*;
    use mf_manifest::parse;

    #[test]
    fn test_build_simple_metric_plan() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["bookings".into()],
            group_by: vec![GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                grain: TimeGrain::Day,
                entity_path: vec![],
            }],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
        };

        let plan = build_plan(&graph, &query).unwrap();

        // The plan should have a sink node
        assert!(plan.sink().is_some());

        // Walk from sink: should be Aggregate with a ReadFromSource parent
        let sink = plan.sink().unwrap();
        match plan.node(sink) {
            DataflowNode::Aggregate {
                group_by,
                aggregations,
            } => {
                assert_eq!(group_by, &["metric_time__day"]);
                assert_eq!(aggregations.len(), 1);
                assert_eq!(aggregations[0].measure_name, "bookings");
                assert_eq!(aggregations[0].agg_type, AggregationType::Sum);
                assert_eq!(aggregations[0].expr, "1");
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }

        let parents = plan.parents(sink);
        assert_eq!(parents.len(), 1);
        match plan.node(parents[0]) {
            DataflowNode::ReadFromSource { model_name, table } => {
                assert_eq!(model_name, "bookings_source");
                assert_eq!(table, "demo.fct_bookings");
            }
            other => panic!("expected ReadFromSource, got {other:?}"),
        }
    }

    #[test]
    fn test_build_simple_metric_with_categorical_dimension() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["bookings".into()],
            group_by: vec![
                GroupBySpec::TimeDimension {
                    name: "metric_time".into(),
                    grain: TimeGrain::Day,
                    entity_path: vec![],
                },
                GroupBySpec::Dimension {
                    name: "is_instant".into(),
                    entity_path: vec![],
                },
            ],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
        };

        let plan = build_plan(&graph, &query).unwrap();
        let sink = plan.sink().unwrap();

        match plan.node(sink) {
            DataflowNode::Aggregate { group_by, .. } => {
                assert!(group_by.contains(&"metric_time__day".to_string()));
                assert!(group_by.contains(&"is_instant".to_string()));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn test_build_unknown_metric_error() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["nonexistent".into()],
            group_by: vec![],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
        };

        let result = build_plan(&graph, &query);
        assert!(result.is_err());
    }
}
