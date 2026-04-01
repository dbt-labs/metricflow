use crate::ast::*;
use mf_core::types::AggregationType;
use mf_manifest::graph::SemanticGraph;
use mf_planning::dataflow::*;
use petgraph::graph::NodeIndex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("plan has no sink node")]
    NoSink,
    #[error("unexpected node type during conversion: {0}")]
    UnexpectedNode(String),
}

/// Convert a DataflowPlan to a SqlSelect AST.
/// Uses the SemanticGraph for resolving dimension/measure metadata.
pub fn to_sql(plan: &DataflowPlan, _graph: &SemanticGraph) -> Result<SqlSelect, ConvertError> {
    to_sql_standalone(plan)
}

/// Convert without graph dependency (for testing and simple cases).
pub fn to_sql_standalone(plan: &DataflowPlan) -> Result<SqlSelect, ConvertError> {
    let sink = plan.sink().ok_or(ConvertError::NoSink)?;
    convert_node(plan, sink, &mut 0)
}

fn convert_node(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    subquery_counter: &mut u32,
) -> Result<SqlSelect, ConvertError> {
    match plan.node(node_idx) {
        DataflowNode::ReadFromSource { model_name, table } => {
            convert_read_source(model_name, table, subquery_counter)
        }
        DataflowNode::Aggregate {
            group_by,
            aggregations,
        } => {
            let parents = plan.parents(node_idx);
            let parent_sql = convert_node(plan, parents[0], subquery_counter)?;
            convert_aggregate(&parent_sql, group_by, aggregations, subquery_counter)
        }
        DataflowNode::OrderBy { specs } => {
            let parents = plan.parents(node_idx);
            let mut parent_sql = convert_node(plan, parents[0], subquery_counter)?;
            parent_sql.order_by = specs
                .iter()
                .map(|(col, desc)| SqlOrderBy {
                    expr: SqlExpr::Literal(col.clone()),
                    descending: *desc,
                })
                .collect();
            Ok(parent_sql)
        }
        DataflowNode::Limit { count } => {
            let parents = plan.parents(node_idx);
            let mut parent_sql = convert_node(plan, parents[0], subquery_counter)?;
            parent_sql.limit = Some(*count);
            Ok(parent_sql)
        }
        other => Err(ConvertError::UnexpectedNode(format!("{other:?}"))),
    }
}

fn convert_read_source(
    _model_name: &str,
    table: &str,
    _subquery_counter: &mut u32,
) -> Result<SqlSelect, ConvertError> {
    let alias = format!("{table}_src");

    // Inner select: SELECT * FROM table alias
    // (The actual columns will be specified by the parent Aggregate node)
    Ok(SqlSelect {
        select_columns: vec![SqlExpr::Literal("*".into())],
        from: SqlFrom::Table {
            table: table.to_string(),
            alias,
        },
        joins: vec![],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}

fn convert_aggregate(
    source: &SqlSelect,
    group_by: &[String],
    aggregations: &[MeasureAggregation],
    subquery_counter: &mut u32,
) -> Result<SqlSelect, ConvertError> {
    let subq_alias = format!("subq_{subquery_counter}");
    *subquery_counter += 1;

    // Build inner SELECT: select measure expressions and group-by columns from source
    let mut inner_columns = Vec::new();

    // Add group-by columns to inner select
    for col in group_by {
        inner_columns.push(SqlExpr::Alias {
            expr: Box::new(match &source.from {
                SqlFrom::Table { alias, .. } => SqlExpr::ColumnRef {
                    table_alias: alias.clone(),
                    column_name: col.clone(),
                },
                SqlFrom::Subquery { alias, .. } => SqlExpr::ColumnRef {
                    table_alias: alias.clone(),
                    column_name: col.clone(),
                },
            }),
            alias: col.clone(),
        });
    }

    // Add measure expressions to inner select with __ prefix
    for agg in aggregations {
        let internal_alias = format!("__{}", agg.measure_name);
        inner_columns.push(SqlExpr::Alias {
            expr: Box::new(SqlExpr::Literal(agg.expr.clone())),
            alias: internal_alias,
        });
    }

    let inner_select = SqlSelect {
        select_columns: inner_columns,
        from: source.from.clone(),
        joins: source.joins.clone(),
        where_clause: source.where_clause.clone(),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    };

    // Build outer SELECT: aggregate + group by
    let mut outer_columns = Vec::new();

    // Group-by columns pass through
    for col in group_by {
        outer_columns.push(SqlExpr::ColumnRef {
            table_alias: subq_alias.clone(),
            column_name: col.clone(),
        });
    }

    // Aggregated measures
    for agg in aggregations {
        let internal_col = format!("__{}", agg.measure_name);
        let agg_func = match agg.agg_type {
            AggregationType::Sum => "SUM",
            AggregationType::Min => "MIN",
            AggregationType::Max => "MAX",
            AggregationType::Count => "COUNT",
            AggregationType::CountDistinct => "COUNT",
            AggregationType::Average => "AVG",
            AggregationType::SumBoolean => "SUM",
            AggregationType::Median => "PERCENTILE_CONT",
            AggregationType::Percentile => "PERCENTILE_CONT",
        };
        let is_distinct = agg.agg_type == AggregationType::CountDistinct;

        outer_columns.push(SqlExpr::Alias {
            expr: Box::new(SqlExpr::AggregateFunction {
                function: agg_func.into(),
                arg: Box::new(SqlExpr::ColumnRef {
                    table_alias: subq_alias.clone(),
                    column_name: internal_col,
                }),
                distinct: is_distinct,
            }),
            alias: agg.alias.clone(),
        });
    }

    let outer_group_by: Vec<SqlExpr> = group_by
        .iter()
        .map(|col| SqlExpr::ColumnRef {
            table_alias: subq_alias.clone(),
            column_name: col.clone(),
        })
        .collect();

    Ok(SqlSelect {
        select_columns: outer_columns,
        from: SqlFrom::Subquery {
            query: Box::new(inner_select),
            alias: subq_alias,
        },
        joins: vec![],
        where_clause: None,
        group_by: outer_group_by,
        order_by: vec![],
        limit: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mf_core::types::AggregationType;
    use mf_planning::dataflow::{DataflowNode, DataflowPlan, MeasureAggregation};

    fn build_test_plan() -> DataflowPlan {
        let mut plan = DataflowPlan::new();

        let read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });

        let agg = plan.add_node(DataflowNode::Aggregate {
            group_by: vec!["metric_time__day".into()],
            aggregations: vec![MeasureAggregation {
                measure_name: "bookings".into(),
                agg_type: AggregationType::Sum,
                expr: "1".into(),
                alias: "bookings".into(),
            }],
        });
        plan.add_edge(read, agg);
        plan.set_sink(agg);
        plan
    }

    #[test]
    fn test_convert_simple_aggregate() {
        let plan = build_test_plan();
        let sql = to_sql_standalone(&plan).unwrap();

        // Should wrap the read in a subquery
        match &sql.from {
            SqlFrom::Subquery { alias, .. } => {
                assert!(alias.starts_with("subq_"));
            }
            SqlFrom::Table { .. } => panic!("expected subquery, got table"),
        }
        // Should have one select column (the aggregated metric) plus group-by columns
        assert!(!sql.select_columns.is_empty());
        assert_eq!(sql.group_by.len(), 1);
    }

    #[test]
    fn test_convert_no_sink_error() {
        let plan = DataflowPlan::new();
        let result = to_sql_standalone(&plan);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConvertError::NoSink));
    }
}
