use crate::dataflow::*;
use crate::resolve::{self, ResolveError};
use mf_core::spec::*;
use mf_core::types::*;
use mf_manifest::graph::SemanticGraph;
use petgraph::graph::NodeIndex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error("only simple metrics are supported in this version")]
    UnsupportedMetricType,
    #[error("dimension '{0}' not found on model '{1}' or any joined model")]
    DimensionNotFound(String, String),
    #[error("no time spine configured for grain '{0}'")]
    NoTimeSpine(String),
    #[error("invalid time grain '{0}': {1}")]
    InvalidTimeGrain(String, String),
}

/// Build a dataflow plan for the given query.
/// Phase 4: supports simple, derived, and ratio metrics.
pub fn build_plan(graph: &SemanticGraph, query: &QuerySpec) -> Result<DataflowPlan, PlanError> {
    // For now: support exactly one metric
    // (Multi-metric queries are a later phase)
    if query.metrics.len() != 1 {
        return Err(PlanError::UnsupportedMetricType);
    }

    let metric_name = &query.metrics[0];
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.clone()))?;

    let mut plan = DataflowPlan::new();

    match metric.metric_type {
        MetricKind::Simple => {
            let current =
                build_simple_metric_plan(graph, query, metric_name, metric_name, &mut plan)?;
            let current = add_order_by_and_limit(&mut plan, current, query);
            plan.set_sink(current);
        }
        MetricKind::Derived => {
            let current = build_derived_metric_plan(graph, query, metric_name, &mut plan)?;
            let current = add_order_by_and_limit(&mut plan, current, query);
            plan.set_sink(current);
        }
        MetricKind::Ratio => {
            let current = build_ratio_metric_plan(graph, query, metric_name, &mut plan)?;
            let current = add_order_by_and_limit(&mut plan, current, query);
            plan.set_sink(current);
        }
        MetricKind::Cumulative => {
            let current = build_cumulative_metric_plan(graph, query, metric_name, &mut plan)?;
            let current = add_order_by_and_limit(&mut plan, current, query);
            plan.set_sink(current);
        }
        _ => return Err(PlanError::UnsupportedMetricType),
    }

    Ok(plan)
}

/// Build the subgraph for a simple metric (ReadFromSource → optional joins → Aggregate).
/// Returns the NodeIndex of the Aggregate node.
fn build_simple_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
    output_alias: &str,
    plan: &mut DataflowPlan,
) -> Result<NodeIndex, PlanError> {
    let agg_node = build_input_metric_subplan(graph, query, metric_name, output_alias, plan)?;
    Ok(agg_node)
}

/// Build a subgraph for an input metric used by derived/ratio metrics.
/// Returns the NodeIndex of the Aggregate node.
fn build_input_metric_subplan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
    output_alias: &str,
    plan: &mut DataflowPlan,
) -> Result<NodeIndex, PlanError> {
    let resolved = resolve::resolve_simple_metric(graph, metric_name)?;
    let left_model_name = resolved.model.name.as_str();

    // Step 1: ReadFromSource for the primary (measure) model
    let read_node = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    // Step 2: Determine which group-by columns require joins.
    let mut join_nodes: std::collections::HashMap<String, NodeIndex> =
        std::collections::HashMap::new();

    // Validate all dimensions and prepare join nodes
    for group in &query.group_by {
        let dim_name = match group {
            GroupBySpec::Dimension { name, .. } => name.as_str(),
            GroupBySpec::TimeDimension { name, .. } => {
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

    // Step 3: Build group-by columns with resolved SQL expressions
    let group_by_columns: Vec<GroupByColumn> = query
        .group_by
        .iter()
        .map(|g| match g {
            GroupBySpec::TimeDimension {
                name, grain, entity_path, ..
            } => {
                let alias = g.column_name();
                if name == "metric_time" {
                    // Resolve metric_time to the agg_time_dimension's SQL expression
                    let dim_expr = resolved
                        .agg_time_dimension
                        .map(|d| d.sql_expr().to_string())
                        .unwrap_or_else(|| name.clone());
                    GroupByColumn {
                        alias,
                        expr: format!("DATE_TRUNC('{grain}', {dim_expr})"),
                    }
                } else {
                    // Non-metric_time time dimension: find on model or joined model
                    let dim_name = name.as_str();
                    let model_name = if entity_path.is_empty() {
                        left_model_name
                    } else {
                        // If there's an entity path, the dimension is on a joined model
                        graph
                            .find_join_path(left_model_name, dim_name)
                            .map(|j| j.right_model.name.as_str())
                            .unwrap_or(left_model_name)
                    };
                    let dim_expr = graph
                        .find_dimension(dim_name, model_name)
                        .map(|(_, d)| d.sql_expr().to_string())
                        .unwrap_or_else(|| dim_name.to_string());
                    GroupByColumn {
                        alias,
                        expr: format!("DATE_TRUNC('{grain}', {dim_expr})"),
                    }
                }
            }
            GroupBySpec::Dimension {
                name, entity_path, ..
            } => {
                let alias = g.column_name();
                let dim_name = name.as_str();
                let model_name = if entity_path.is_empty() {
                    left_model_name
                } else {
                    graph
                        .find_join_path(left_model_name, dim_name)
                        .map(|j| j.right_model.name.as_str())
                        .unwrap_or(left_model_name)
                };
                let dim_expr = graph
                    .find_dimension(dim_name, model_name)
                    .map(|(_, d)| d.sql_expr().to_string())
                    .unwrap_or_else(|| dim_name.to_string());
                GroupByColumn {
                    alias,
                    expr: dim_expr,
                }
            }
            GroupBySpec::Entity { .. } => GroupByColumn::simple(g.column_name()),
        })
        .collect();

    let aggregations = vec![MeasureAggregation {
        measure_name: resolved.measure.name.clone(),
        agg_type: resolved.measure.agg,
        expr: resolved.measure.expr.clone(),
        alias: output_alias.to_string(),
    }];

    // Step 4: Aggregate node.
    let agg_input = if join_nodes.is_empty() {
        read_node
    } else {
        *join_nodes.values().next().unwrap()
    };

    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations,
    });
    plan.add_edge(agg_input, agg_node);

    Ok(agg_node)
}

/// Add optional ORDER BY and LIMIT nodes to the plan, returning the final node.
fn add_order_by_and_limit(
    plan: &mut DataflowPlan,
    current: NodeIndex,
    query: &QuerySpec,
) -> NodeIndex {
    let mut current = current;

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

    if let Some(count) = query.limit {
        let limit_node = plan.add_node(DataflowNode::Limit { count });
        plan.add_edge(current, limit_node);
        current = limit_node;
    }

    current
}

/// Build the plan for a derived metric.
/// Returns the NodeIndex of the ComputeMetric node.
fn build_derived_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
    plan: &mut DataflowPlan,
) -> Result<NodeIndex, PlanError> {
    let resolved = resolve::resolve_derived_metric(graph, metric_name)?;

    // Build a subplan for each input metric
    let mut agg_nodes: Vec<NodeIndex> = Vec::new();
    for (input_metric_name, alias) in &resolved.inputs {
        let agg_node = build_input_metric_subplan(graph, query, input_metric_name, alias, plan)?;
        agg_nodes.push(agg_node);
    }

    // If 2+ inputs: create CombineAggregatedOutputs node
    let combine_or_agg = if agg_nodes.len() >= 2 {
        let combine = plan.add_node(DataflowNode::CombineAggregatedOutputs);
        for &agg in &agg_nodes {
            plan.add_edge(agg, combine);
        }
        combine
    } else {
        agg_nodes[0]
    };

    // ComputeMetric node with the expression
    let compute = plan.add_node(DataflowNode::ComputeMetric {
        metric_name: metric_name.to_string(),
        expr: Some(resolved.expr.clone()),
    });
    plan.add_edge(combine_or_agg, compute);

    Ok(compute)
}

/// Build the plan for a ratio metric.
/// Returns the NodeIndex of the ComputeMetric node.
fn build_ratio_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
    plan: &mut DataflowPlan,
) -> Result<NodeIndex, PlanError> {
    let resolved = resolve::resolve_ratio_metric(graph, metric_name)?;

    let (num_metric_name, num_alias) = &resolved.numerator;
    let (den_metric_name, den_alias) = &resolved.denominator;

    // Build numerator and denominator subplans
    let num_agg = build_input_metric_subplan(graph, query, num_metric_name, num_alias, plan)?;
    let den_agg = build_input_metric_subplan(graph, query, den_metric_name, den_alias, plan)?;

    // Combine the two via CombineAggregatedOutputs
    let combine = plan.add_node(DataflowNode::CombineAggregatedOutputs);
    plan.add_edge(num_agg, combine);
    plan.add_edge(den_agg, combine);

    // ComputeMetric: CAST(num / NULLIF(den, 0))
    let expr = format!("CAST({num_alias} AS DOUBLE) / CAST(NULLIF({den_alias}, 0) AS DOUBLE)");
    let compute = plan.add_node(DataflowNode::ComputeMetric {
        metric_name: metric_name.to_string(),
        expr: Some(expr),
    });
    plan.add_edge(combine, compute);

    Ok(compute)
}

/// Build the plan for a cumulative metric.
/// Plan: ReadFromSource → JoinOverTimeRange → Aggregate.
/// Returns the NodeIndex of the Aggregate node.
fn build_cumulative_metric_plan(
    graph: &SemanticGraph,
    query: &QuerySpec,
    metric_name: &str,
    plan: &mut DataflowPlan,
) -> Result<NodeIndex, PlanError> {
    let resolved = resolve::resolve_cumulative_metric(graph, metric_name)?;

    // Determine the query grain (default to Day if not specified).
    let query_grain = query
        .group_by
        .iter()
        .find_map(|g| match g {
            GroupBySpec::TimeDimension { grain, .. } => Some(*grain),
            _ => None,
        })
        .unwrap_or(TimeGrain::Day);

    // Find time spine for the query grain.
    let time_spine = graph
        .find_time_spine(query_grain)
        .ok_or_else(|| PlanError::NoTimeSpine(query_grain.to_string()))?;

    // Convert MetricTimeWindow → TimeWindow if present.
    let window = if let Some(mw) = resolved.window {
        let grain = mw
            .granularity
            .parse::<TimeGrain>()
            .map_err(|e| PlanError::InvalidTimeGrain(mw.granularity.clone(), e))?;
        Some(TimeWindow {
            count: mw.count as u64,
            grain,
        })
    } else {
        None
    };

    // Determine metric_time_column from agg_time_dimension or fallback to time spine column.
    let metric_time_column = resolved
        .agg_time_dimension
        .map(|d| d.sql_expr().to_string())
        .unwrap_or_else(|| time_spine.column.clone());

    // Step 1: ReadFromSource for the measure model.
    let read_node = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    // Step 2: JoinOverTimeRange (join source against time spine).
    let join_node = plan.add_node(DataflowNode::JoinOverTimeRange {
        time_spine_table: time_spine.table,
        time_spine_column: time_spine.column,
        time_spine_grain: time_spine.grain,
        window,
        grain_to_date: resolved.grain_to_date,
        metric_time_column,
    });
    plan.add_edge(read_node, join_node);

    // Step 3: Build group-by columns.
    // For cumulative metrics, metric_time comes from the time spine (handled by JoinOverTimeRange),
    // so we use simple column aliases here.
    let group_by_columns: Vec<GroupByColumn> = query
        .group_by
        .iter()
        .map(|g| GroupByColumn::simple(g.column_name()))
        .collect();

    let aggregations = vec![MeasureAggregation {
        measure_name: resolved.measure.name.clone(),
        agg_type: resolved.measure.agg,
        expr: resolved.measure.expr.clone(),
        alias: metric_name.to_string(),
    }];

    // Step 4: Aggregate node.
    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations,
    });
    plan.add_edge(join_node, agg_node);

    Ok(agg_node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::DataflowNode;
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
                let aliases: Vec<&str> = group_by.iter().map(|g| g.alias.as_str()).collect();
                assert_eq!(aliases, &["metric_time__day"]);
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
                let aliases: Vec<&str> = group_by.iter().map(|g| g.alias.as_str()).collect();
                assert!(aliases.contains(&"metric_time__day"));
                assert!(aliases.contains(&"is_instant"));
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
                let aliases: Vec<&str> = group_by.iter().map(|g| g.alias.as_str()).collect();
                assert!(aliases.contains(&"listing__country"));
                assert!(aliases.contains(&"metric_time__day"));
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

    #[test]
    fn test_build_derived_metric_plan() {
        let json = include_str!("../../../tests/fixtures/derived_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["bookings_growth".into()],
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
        assert!(plan.sink().is_some());

        // Sink should be ComputeMetric
        let sink = plan.sink().unwrap();
        match plan.node(sink) {
            DataflowNode::ComputeMetric { metric_name, expr } => {
                assert_eq!(metric_name, "bookings_growth");
                assert_eq!(expr.as_deref(), Some("bookings - instant_bookings"));
            }
            other => panic!("expected ComputeMetric, got {other:?}"),
        }

        // Parent of ComputeMetric should be CombineAggregatedOutputs
        let compute_parents = plan.parents(sink);
        assert_eq!(compute_parents.len(), 1);
        match plan.node(compute_parents[0]) {
            DataflowNode::CombineAggregatedOutputs => {}
            other => panic!("expected CombineAggregatedOutputs, got {other:?}"),
        }

        // CombineAggregatedOutputs should have 2 Aggregate parents
        let combine_parents = plan.parents(compute_parents[0]);
        assert_eq!(combine_parents.len(), 2);
        for &parent in &combine_parents {
            match plan.node(parent) {
                DataflowNode::Aggregate { .. } => {}
                other => panic!("expected Aggregate, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_build_ratio_metric_plan() {
        let json = include_str!("../../../tests/fixtures/ratio_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["instant_booking_rate".into()],
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
        assert!(plan.sink().is_some());

        // Sink should be ComputeMetric
        let sink = plan.sink().unwrap();
        match plan.node(sink) {
            DataflowNode::ComputeMetric { metric_name, expr } => {
                assert_eq!(metric_name, "instant_booking_rate");
                let expr_str = expr.as_deref().unwrap_or("");
                assert!(
                    expr_str.contains("NULLIF"),
                    "expr should contain NULLIF: {expr_str}"
                );
                assert!(
                    expr_str.contains("CAST"),
                    "expr should contain CAST: {expr_str}"
                );
            }
            other => panic!("expected ComputeMetric, got {other:?}"),
        }

        // Parent should be CombineAggregatedOutputs
        let compute_parents = plan.parents(sink);
        assert_eq!(compute_parents.len(), 1);
        match plan.node(compute_parents[0]) {
            DataflowNode::CombineAggregatedOutputs => {}
            other => panic!("expected CombineAggregatedOutputs, got {other:?}"),
        }
    }

    #[test]
    fn test_build_cumulative_metric_plan_trailing_window() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["trailing_7d_bookings".into()],
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
        assert!(plan.sink().is_some());

        // Sink should be Aggregate (3 nodes: ReadFromSource → JoinOverTimeRange → Aggregate)
        let sink = plan.sink().unwrap();
        match plan.node(sink) {
            DataflowNode::Aggregate { aggregations, .. } => {
                assert_eq!(aggregations[0].alias, "trailing_7d_bookings");
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }

        // Parent of Aggregate should be JoinOverTimeRange
        let parents = plan.parents(sink);
        assert_eq!(parents.len(), 1);
        match plan.node(parents[0]) {
            DataflowNode::JoinOverTimeRange {
                time_spine_table,
                window,
                grain_to_date,
                ..
            } => {
                assert_eq!(time_spine_table, "demo.mf_time_spine");
                assert!(window.is_some());
                let w = window.as_ref().unwrap();
                assert_eq!(w.count, 7);
                assert_eq!(w.grain, TimeGrain::Day);
                assert!(grain_to_date.is_none());
            }
            other => panic!("expected JoinOverTimeRange, got {other:?}"),
        }

        assert_eq!(plan.node_count(), 3);
    }

    #[test]
    fn test_build_cumulative_metric_plan_grain_to_date() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["bookings_mtd".into()],
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
        let sink = plan.sink().unwrap();
        let parents = plan.parents(sink);
        match plan.node(parents[0]) {
            DataflowNode::JoinOverTimeRange {
                window,
                grain_to_date,
                ..
            } => {
                assert!(window.is_none());
                assert_eq!(*grain_to_date, Some(TimeGrain::Month));
            }
            other => panic!("expected JoinOverTimeRange, got {other:?}"),
        }
    }

    #[test]
    fn test_build_cumulative_metric_plan_all_time() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let query = QuerySpec {
            metrics: vec!["bookings_all_time".into()],
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
        let sink = plan.sink().unwrap();
        let parents = plan.parents(sink);
        match plan.node(parents[0]) {
            DataflowNode::JoinOverTimeRange {
                window,
                grain_to_date,
                ..
            } => {
                assert!(window.is_none());
                assert!(grain_to_date.is_none());
            }
            other => panic!("expected JoinOverTimeRange, got {other:?}"),
        }
    }
}
