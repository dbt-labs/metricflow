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
    #[error("dimension '{0}' not found on model '{1}' or any joined model")]
    DimensionNotFound(String, String),
}

/// Build a dataflow plan for the given query.
/// Phase 3: supports simple metrics with dimensions on the same model OR via a
/// one-hop entity join (FOREIGN → PRIMARY on the same entity name).
pub fn build_plan(graph: &SemanticGraph, query: &QuerySpec) -> Result<DataflowPlan, PlanError> {
    // For now: support exactly one metric
    // (Multi-metric queries are a later phase)
    if query.metrics.len() != 1 {
        return Err(PlanError::UnsupportedMetricType);
    }

    let metric_name = &query.metrics[0];
    let resolved = resolve::resolve_simple_metric(graph, metric_name)?;

    let mut plan = DataflowPlan::new();
    let left_model_name = resolved.model.name.as_str();

    // Step 1: ReadFromSource for the primary (measure) model
    let read_node = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    // Step 2: Determine which group-by columns require joins.
    // Collect unique (entity_name, right_model) pairs that are needed.
    // Key: entity_name; value: (join node index) — built lazily.
    let mut join_nodes: std::collections::HashMap<String, petgraph::graph::NodeIndex> =
        std::collections::HashMap::new();

    // Validate all dimensions and prepare join nodes
    for group in &query.group_by {
        let dim_name = match group {
            GroupBySpec::Dimension { name, .. } => name.as_str(),
            GroupBySpec::TimeDimension { name, .. } => {
                // metric_time is a virtual name; underlying dimension name is the agg_time_dimension
                if name == "metric_time" {
                    continue; // handled via agg_time_dimension; no join needed
                }
                name.as_str()
            }
            GroupBySpec::Entity { .. } => continue,
        };

        // Check if the dimension is local
        if graph.find_dimension(dim_name, left_model_name).is_some() {
            continue; // local, no join needed
        }

        // Check if a one-hop join exists
        match graph.find_join_path(left_model_name, dim_name) {
            Some(join) => {
                let entity_name = join.entity_name.to_string();
                if join_nodes.contains_key(&entity_name) {
                    continue; // join node for this entity already created
                }

                // Create ReadFromSource for the right model
                let right_read = plan.add_node(DataflowNode::ReadFromSource {
                    model_name: join.right_model.name.clone(),
                    table: join.right_model.node_relation.fully_qualified(),
                });

                // Create JoinOnEntities node
                let join_node = plan.add_node(DataflowNode::JoinOnEntities {
                    entity_name: entity_name.clone(),
                    left_key: join.left_expr.to_string(),
                    right_key: join.right_expr.to_string(),
                    right_model_name: join.right_model.name.clone(),
                });

                // Edges: left_read → join, right_read → join
                plan.add_edge(read_node, join_node);
                plan.add_edge(right_read, join_node);

                join_nodes.insert(entity_name, join_node);
            }
            None => {
                return Err(PlanError::DimensionNotFound(
                    dim_name.into(),
                    left_model_name.into(),
                ));
            }
        }
    }

    // Step 3: Build group-by column names
    let group_by_columns: Vec<String> = query.group_by.iter().map(|g| g.column_name()).collect();

    let aggregations = vec![MeasureAggregation {
        measure_name: resolved.measure.name.clone(),
        agg_type: resolved.measure.agg,
        expr: resolved.measure.sql_expr().to_string(),
        alias: resolved.metric.name.clone(),
    }];

    // Step 4: Aggregate node. Its parent is either the last join node or the read node.
    let agg_input = if join_nodes.is_empty() {
        read_node
    } else {
        // Use the last join node as aggregate input.
        // (In a multi-join scenario we'd chain joins; for Phase 3 single-hop is enough.)
        *join_nodes.values().next().unwrap()
    };

    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations,
    });
    plan.add_edge(agg_input, agg_node);

    // Step 5: Optional ORDER BY
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

    // Step 6: Optional LIMIT
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

    #[test]
    fn test_build_plan_with_join_dimension() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        // Query for bookings metric grouped by listing__country (cross-model join)
        let query = QuerySpec {
            metrics: vec!["bookings".into()],
            group_by: vec![
                GroupBySpec::TimeDimension {
                    name: "metric_time".into(),
                    grain: TimeGrain::Day,
                    entity_path: vec![],
                },
                GroupBySpec::Dimension {
                    name: "country".into(),
                    entity_path: vec!["listing".into()],
                },
            ],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
        };

        let plan = build_plan(&graph, &query).unwrap();
        assert!(plan.sink().is_some());

        // Plan should have: 2 ReadFromSource + 1 JoinOnEntities + 1 Aggregate = 4 nodes
        assert_eq!(plan.node_count(), 4);

        // Sink should be the Aggregate node
        let sink = plan.sink().unwrap();
        match plan.node(sink) {
            DataflowNode::Aggregate { group_by, .. } => {
                assert!(group_by.contains(&"listing__country".to_string()));
                assert!(group_by.contains(&"metric_time__day".to_string()));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }

        // Parent of aggregate should be JoinOnEntities
        let agg_parents = plan.parents(sink);
        assert_eq!(agg_parents.len(), 1);
        match plan.node(agg_parents[0]) {
            DataflowNode::JoinOnEntities {
                entity_name,
                left_key,
                right_key,
                right_model_name,
            } => {
                assert_eq!(entity_name, "listing");
                assert_eq!(left_key, "listing_id");
                assert_eq!(right_key, "listing_id");
                assert_eq!(right_model_name, "listings_source");
            }
            other => panic!("expected JoinOnEntities, got {other:?}"),
        }

        // JoinOnEntities should have 2 parents (left ReadFromSource, right ReadFromSource)
        let join_parents = plan.parents(agg_parents[0]);
        assert_eq!(join_parents.len(), 2);
    }

    #[test]
    fn test_build_plan_unknown_dimension_error() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["bookings".into()],
            group_by: vec![GroupBySpec::Dimension {
                name: "nonexistent_dim".into(),
                entity_path: vec![],
            }],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
        };

        let result = build_plan(&graph, &query);
        assert!(result.is_err());
        match result.unwrap_err() {
            PlanError::DimensionNotFound(dim, _) => assert_eq!(dim, "nonexistent_dim"),
            other => panic!("expected DimensionNotFound, got {other:?}"),
        }
    }

    #[test]
    fn test_build_plan_local_dimension_no_join() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        // is_instant is on bookings_source — no join should be emitted
        let query = QuerySpec {
            metrics: vec!["bookings".into()],
            group_by: vec![GroupBySpec::Dimension {
                name: "is_instant".into(),
                entity_path: vec![],
            }],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
        };

        let plan = build_plan(&graph, &query).unwrap();
        // 1 ReadFromSource + 1 Aggregate = 2 nodes (no join)
        assert_eq!(plan.node_count(), 2);

        let sink = plan.sink().unwrap();
        match plan.node(sink) {
            DataflowNode::Aggregate { .. } => {}
            other => panic!("expected Aggregate, got {other:?}"),
        }
        let parents = plan.parents(sink);
        assert_eq!(parents.len(), 1);
        match plan.node(parents[0]) {
            DataflowNode::ReadFromSource { model_name, .. } => {
                assert_eq!(model_name, "bookings_source");
            }
            other => panic!("expected ReadFromSource, got {other:?}"),
        }
    }
}
