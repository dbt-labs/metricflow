use crate::ast::*;
use mf_core::types::{AggregationType, TimeGrain, TimeWindow};
use mf_manifest::graph::SemanticGraph;
use mf_planning::dataflow::*;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
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
pub fn to_sql(plan: &DataflowPlan, graph: &SemanticGraph) -> Result<SqlSelect, ConvertError> {
    let sink = plan.sink().ok_or(ConvertError::NoSink)?;
    convert_node(plan, sink, &mut 0, Some(graph))
}

/// Convert without graph dependency (for testing and simple cases).
pub fn to_sql_standalone(plan: &DataflowPlan) -> Result<SqlSelect, ConvertError> {
    let sink = plan.sink().ok_or(ConvertError::NoSink)?;
    convert_node(plan, sink, &mut 0, None)
}

fn convert_node<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
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
            let parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;
            convert_aggregate(&parent_sql, group_by, aggregations, subquery_counter)
        }
        DataflowNode::OrderBy { specs } => {
            let parents = plan.parents(node_idx);
            let mut parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;
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
            let mut parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;
            parent_sql.limit = Some(*count);
            Ok(parent_sql)
        }
        DataflowNode::JoinOnEntities {
            entity_name,
            left_key,
            right_key,
            right_model_name,
        } => convert_join_on_entities(
            plan,
            node_idx,
            entity_name,
            left_key,
            right_key,
            right_model_name,
            subquery_counter,
            graph,
        ),
        DataflowNode::JoinOverTimeRange {
            time_spine_table,
            time_spine_column,
            time_spine_grain,
            window,
            grain_to_date,
            metric_time_column,
        } => convert_join_over_time_range(
            plan,
            node_idx,
            time_spine_table,
            time_spine_column,
            *time_spine_grain,
            window.as_ref(),
            *grain_to_date,
            metric_time_column,
            subquery_counter,
            graph,
        ),
        DataflowNode::CombineAggregatedOutputs => {
            convert_combine_aggregated_outputs(plan, node_idx, subquery_counter, graph)
        }
        DataflowNode::ComputeMetric { metric_name, expr } => {
            convert_compute_metric(plan, node_idx, metric_name, expr, subquery_counter, graph)
        }
        DataflowNode::WhereFilter { filters } => {
            let parents = plan.parents(node_idx);
            let parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;
            convert_where_filter(&parent_sql, filters, subquery_counter)
        }
        other => Err(ConvertError::UnexpectedNode(format!("{other:?}"))),
    }
}

#[allow(clippy::too_many_arguments)]
fn convert_join_on_entities<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    entity_name: &str,
    left_key: &str,
    right_key: &str,
    right_model_name: &str,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    let parents = plan.parents(node_idx);

    // Identify left vs right parent by matching right_model_name.
    let (left_idx, right_idx) = {
        let mut left = None;
        let mut right = None;
        for &p in &parents {
            match plan.node(p) {
                DataflowNode::ReadFromSource { model_name, .. }
                    if model_name == right_model_name =>
                {
                    right = Some(p);
                }
                _ => {
                    left = Some(p);
                }
            }
        }
        (
            left.ok_or_else(|| {
                ConvertError::UnexpectedNode("JoinOnEntities missing left parent".into())
            })?,
            right.ok_or_else(|| {
                ConvertError::UnexpectedNode("JoinOnEntities missing right parent".into())
            })?,
        )
    };

    let left_sql = convert_node(plan, left_idx, subquery_counter, graph)?;
    let right_sql = convert_node(plan, right_idx, subquery_counter, graph)?;

    // Extract aliases for the ON clause
    let left_alias = match &left_sql.from {
        SqlFrom::Table { alias, .. } => alias.clone(),
        SqlFrom::Subquery { alias, .. } => alias.clone(),
    };
    let right_alias = match &right_sql.from {
        SqlFrom::Table { alias, .. } => alias.clone(),
        SqlFrom::Subquery { alias, .. } => alias.clone(),
    };

    // Build ON expression: left_alias.left_key = right_alias.right_key
    let on_expr = SqlExpr::Literal(format!(
        "{left_alias}.{left_key} = {right_alias}.{right_key}"
    ));

    let join = SqlJoin {
        join_type: "LEFT OUTER JOIN".into(),
        source: right_sql.from.clone(),
        on: on_expr,
    };

    // Build a flat SELECT that projects left columns as * plus renames
    // right-side dimension columns to entity__dimension format.
    // Wrap in a subquery so the Aggregate node above can reference
    // logical column names (e.g. listing__country) uniformly.
    let join_subq_alias = format!("join_subq_{subquery_counter}");
    *subquery_counter += 1;

    let mut join_select_columns: Vec<SqlExpr> = vec![
        // Pass through everything from left side
        SqlExpr::Literal(format!("{left_alias}.*")),
    ];

    // Project right-side dimension columns with entity__ prefix alias.
    // Use graph to enumerate right model dimensions if available.
    if let Some(g) = graph
        && let Some(right_model) = g.find_model(right_model_name)
    {
        for dim in &right_model.dimensions {
            let physical_col = dim.sql_expr();
            let logical_alias = format!("{entity_name}__{}", dim.name);
            join_select_columns.push(SqlExpr::Alias {
                expr: Box::new(SqlExpr::ColumnRef {
                    table_alias: right_alias.clone(),
                    column_name: physical_col.to_string(),
                }),
                alias: logical_alias,
            });
        }
    }

    let join_inner = SqlSelect {
        select_columns: join_select_columns,
        from: left_sql.from.clone(),
        joins: vec![join],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    };

    // Return a passthrough SELECT * from the join subquery.
    // The Aggregate node above will build the actual column projections using join_subq_alias.
    Ok(SqlSelect {
        select_columns: vec![SqlExpr::Literal("*".into())],
        from: SqlFrom::Subquery {
            query: Box::new(join_inner),
            alias: join_subq_alias,
        },
        joins: vec![],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}

fn convert_read_source(
    _model_name: &str,
    table: &str,
    _subquery_counter: &mut u32,
) -> Result<SqlSelect, ConvertError> {
    // Use only the last segment of a fully qualified table name for the alias
    // e.g., "db.schema.table" → "table_src"
    let table_leaf = table.rsplit('.').next().unwrap_or(table);
    let alias = format!("{table_leaf}_src");

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

/// Convert a WhereFilter node: project required filter columns from the source,
/// apply the WHERE clause, and pass through all existing columns.
/// Following Python MetricFlow: WhereFilter is its own node BEFORE aggregation.
fn convert_where_filter(
    source: &SqlSelect,
    filters: &[ResolvedFilterInfo],
    subquery_counter: &mut u32,
) -> Result<SqlSelect, ConvertError> {
    if filters.is_empty() {
        return Ok(source.clone());
    }

    let subq_alias = format!("filter_subq_{subquery_counter}");
    *subquery_counter += 1;

    // Build SELECT columns: pass through everything from source, plus add
    // required filter columns that aren't already projected.
    let mut select_columns: Vec<SqlExpr> = vec![SqlExpr::Literal("*".into())];

    for filter in filters {
        for (alias, expr) in &filter.required_columns {
            select_columns.push(SqlExpr::Alias {
                expr: Box::new(SqlExpr::Literal(expr.clone())),
                alias: alias.clone(),
            });
        }
    }

    // Build WHERE from all filter clauses ANDed together.
    let filter_clauses: Vec<String> = filters.iter().map(|f| f.sql.clone()).collect();
    let where_sql = filter_clauses.join(" AND ");

    let inner = SqlSelect {
        select_columns,
        from: source.from.clone(),
        joins: source.joins.clone(),
        where_clause: Some(SqlExpr::Literal(where_sql)),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    };

    // Wrap in a passthrough subquery so downstream nodes (Aggregate) see clean column names.
    Ok(SqlSelect {
        select_columns: vec![SqlExpr::Literal("*".into())],
        from: SqlFrom::Subquery {
            query: Box::new(inner),
            alias: subq_alias,
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
    group_by: &[GroupByColumn],
    aggregations: &[MeasureAggregation],
    subquery_counter: &mut u32,
) -> Result<SqlSelect, ConvertError> {
    let subq_alias = format!("subq_{subquery_counter}");
    *subquery_counter += 1;

    // Build inner SELECT: select measure expressions and group-by columns from source
    let mut inner_columns = Vec::new();

    // Add group-by columns to inner select using resolved SQL expressions
    for col in group_by {
        let expr = if col.alias == col.expr {
            // Simple column reference — qualify with source alias
            match &source.from {
                SqlFrom::Table { alias, .. } => SqlExpr::ColumnRef {
                    table_alias: alias.clone(),
                    column_name: col.expr.clone(),
                },
                SqlFrom::Subquery { alias, .. } => SqlExpr::ColumnRef {
                    table_alias: alias.clone(),
                    column_name: col.expr.clone(),
                },
            }
        } else {
            // Resolved SQL expression (e.g., DATE_TRUNC('day', close_month))
            SqlExpr::Literal(col.expr.clone())
        };
        inner_columns.push(SqlExpr::Alias {
            expr: Box::new(expr),
            alias: col.alias.clone(),
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

    // Group-by columns pass through using alias names
    for col in group_by {
        outer_columns.push(SqlExpr::ColumnRef {
            table_alias: subq_alias.clone(),
            column_name: col.alias.clone(),
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

        let agg_expr = SqlExpr::AggregateFunction {
            function: agg_func.into(),
            arg: Box::new(SqlExpr::ColumnRef {
                table_alias: subq_alias.clone(),
                column_name: internal_col,
            }),
            distinct: is_distinct,
        };

        // Wrap in COALESCE(AGG(...), value) if fill_nulls_with is configured
        let final_expr = if let Some(fill_value) = agg.fill_nulls_with {
            SqlExpr::FunctionCall {
                function: "COALESCE".into(),
                args: vec![agg_expr, SqlExpr::Literal(fill_value.to_string())],
            }
        } else {
            agg_expr
        };

        outer_columns.push(SqlExpr::Alias {
            expr: Box::new(final_expr),
            alias: agg.alias.clone(),
        });
    }

    let outer_group_by: Vec<SqlExpr> = group_by
        .iter()
        .map(|col| SqlExpr::ColumnRef {
            table_alias: subq_alias.clone(),
            column_name: col.alias.clone(),
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

/// Walk a plan subtree to find Aggregate nodes and collect fill_nulls_with values
/// for each metric alias. This is used by CombineAggregatedOutputs to wrap metric
/// columns in COALESCE(MAX(...), fill_value) in the re-aggregation.
fn collect_fill_nulls_with(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    out: &mut HashMap<String, i64>,
) {
    match plan.node(node_idx) {
        DataflowNode::Aggregate { aggregations, .. } => {
            for agg in aggregations {
                if let Some(fill_value) = agg.fill_nulls_with {
                    out.insert(agg.alias.clone(), fill_value);
                }
            }
        }
        _ => {
            // Recurse into parents
            for &parent in &plan.parents(node_idx) {
                collect_fill_nulls_with(plan, parent, out);
            }
        }
    }
}

fn convert_combine_aggregated_outputs<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    let parents = plan.parents(node_idx);
    if parents.is_empty() {
        return Err(ConvertError::UnexpectedNode(
            "CombineAggregatedOutputs has no parents".into(),
        ));
    }

    // Convert each parent to SQL
    let mut parent_sqls: Vec<SqlSelect> = Vec::new();
    for &parent in &parents {
        parent_sqls.push(convert_node(plan, parent, subquery_counter, graph)?);
    }

    // Determine group_by column names from first parent's group_by.
    // If the parent has no explicit group_by (e.g., ComputeMetric wraps its result
    // without GROUP BY), infer group-by columns from select_columns: ColumnRef entries
    // are pass-through group-by columns, Alias entries are metric columns.
    let mut group_by_cols: Vec<String> = parent_sqls[0]
        .group_by
        .iter()
        .filter_map(|expr| match expr {
            SqlExpr::ColumnRef { column_name, .. } => Some(column_name.clone()),
            _ => None,
        })
        .collect();

    if group_by_cols.is_empty() {
        group_by_cols = parent_sqls[0]
            .select_columns
            .iter()
            .filter_map(|expr| match expr {
                SqlExpr::ColumnRef { column_name, .. } => Some(column_name.clone()),
                _ => None,
            })
            .collect();
    }

    // Collect metric column names from each parent (select cols that are NOT group-by,
    // and not ColumnRef without alias — only Alias entries whose alias is not in group_by).
    let group_by_set: std::collections::HashSet<&str> =
        group_by_cols.iter().map(|s| s.as_str()).collect();
    let mut metric_cols_per_parent: Vec<Vec<String>> = Vec::new();
    for sql in &parent_sqls {
        let metric_cols: Vec<String> = sql
            .select_columns
            .iter()
            .filter_map(|expr| match expr {
                SqlExpr::Alias { alias, .. } if !group_by_set.contains(alias.as_str()) => {
                    Some(alias.clone())
                }
                _ => None,
            })
            .collect();
        metric_cols_per_parent.push(metric_cols);
    }

    // Extract fill_nulls_with for each metric column by walking the plan.
    // Per Python: simple metric inputs with fill_nulls_with produce
    // COALESCE(MAX(col), fill_value) in the re-aggregation.
    let mut fill_nulls_map: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for &parent in &parents {
        collect_fill_nulls_with(plan, parent, &mut fill_nulls_map);
    }

    // Wrap each parent SqlSelect as a subquery so that it can be used in a join.
    // Assign fresh aliases for the wrapped subqueries.
    let mut wrapped_aliases: Vec<String> = Vec::new();
    let mut wrapped_froms: Vec<SqlFrom> = Vec::new();
    for parent_sql in &parent_sqls {
        let alias = format!("agg_subq_{subquery_counter}");
        *subquery_counter += 1;
        wrapped_froms.push(SqlFrom::Subquery {
            query: Box::new(parent_sql.clone()),
            alias: alias.clone(),
        });
        wrapped_aliases.push(alias);
    }

    let _first_alias = wrapped_aliases[0].clone();

    // Build SELECT columns for the combined subquery:
    // - COALESCE(p1.col, p2.col, ...) AS col for each group-by column
    // - pN.metric_col AS metric_col for each parent's metric column
    let mut select_columns: Vec<SqlExpr> = Vec::new();

    // Group-by columns: COALESCE across all parents
    for col in &group_by_cols {
        let coalesce_args: Vec<SqlExpr> = wrapped_aliases
            .iter()
            .map(|alias| SqlExpr::ColumnRef {
                table_alias: alias.clone(),
                column_name: col.clone(),
            })
            .collect();

        let coalesce_expr = if coalesce_args.len() == 1 {
            coalesce_args.into_iter().next().unwrap()
        } else {
            SqlExpr::FunctionCall {
                function: "COALESCE".into(),
                args: coalesce_args,
            }
        };

        select_columns.push(SqlExpr::Alias {
            expr: Box::new(coalesce_expr),
            alias: col.clone(),
        });
    }

    // Metric columns: project from each parent
    for (i, metric_cols) in metric_cols_per_parent.iter().enumerate() {
        let alias = &wrapped_aliases[i];
        for metric_col in metric_cols {
            select_columns.push(SqlExpr::Alias {
                expr: Box::new(SqlExpr::ColumnRef {
                    table_alias: alias.clone(),
                    column_name: metric_col.clone(),
                }),
                alias: metric_col.clone(),
            });
        }
    }

    // Build FULL OUTER JOINs for remaining parents (or CROSS JOIN if no group_by).
    // Per Python: use COALESCE of previously-seen aliases in the ON condition so that
    // rows that didn't match earlier joins can still match later ones.
    let mut joins: Vec<SqlJoin> = Vec::new();
    for (i, right_from) in wrapped_froms[1..].iter().enumerate() {
        let right_alias = &wrapped_aliases[i + 1];
        let join_type = if group_by_cols.is_empty() {
            "CROSS JOIN".into()
        } else {
            "FULL OUTER JOIN".into()
        };

        let on_expr = if group_by_cols.is_empty() {
            SqlExpr::Literal("1=1".into())
        } else {
            // COALESCE all previously-seen aliases for each group-by column.
            let prev_aliases = &wrapped_aliases[..=i]; // aliases 0..=i (all before right)
            let conditions: Vec<String> = group_by_cols
                .iter()
                .map(|col| {
                    if prev_aliases.len() == 1 {
                        format!("{}.{col} = {right_alias}.{col}", prev_aliases[0])
                    } else {
                        let coalesce_args: Vec<String> = prev_aliases
                            .iter()
                            .map(|a| format!("{a}.{col}"))
                            .collect();
                        format!(
                            "COALESCE({}) = {right_alias}.{col}",
                            coalesce_args.join(", ")
                        )
                    }
                })
                .collect();
            SqlExpr::Literal(conditions.join("\n  AND "))
        };

        joins.push(SqlJoin {
            join_type,
            source: right_from.clone(),
            on: on_expr,
        });
    }

    // Per Python: add a GROUP BY re-aggregation to handle NULL deduplication.
    // NULL = NULL returns NULL in SQL, so FULL OUTER JOINs can produce duplicate
    // NULL rows from each parent. GROUP BY on the COALESCE'd columns + MAX on
    // metric columns merges these into a single row.
    if group_by_cols.is_empty() {
        Ok(SqlSelect {
            select_columns,
            from: wrapped_froms.into_iter().next().unwrap(),
            joins,
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        })
    } else {
        // Build the inner FULL OUTER JOIN query
        let inner = SqlSelect {
            select_columns,
            from: wrapped_froms.into_iter().next().unwrap(),
            joins,
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let reagg_alias = format!("combine_subq_{subquery_counter}");
        *subquery_counter += 1;

        // Outer SELECT: group-by cols as-is, metric cols wrapped in MAX()
        let mut outer_select: Vec<SqlExpr> = Vec::new();
        for col in &group_by_cols {
            outer_select.push(SqlExpr::ColumnRef {
                table_alias: reagg_alias.clone(),
                column_name: col.clone(),
            });
        }
        for metric_cols in &metric_cols_per_parent {
            for metric_col in metric_cols {
                let max_expr = SqlExpr::FunctionCall {
                    function: "MAX".into(),
                    args: vec![SqlExpr::ColumnRef {
                        table_alias: reagg_alias.clone(),
                        column_name: metric_col.clone(),
                    }],
                };
                // Per Python: wrap in COALESCE(MAX(...), fill_value) if fill_nulls_with is set.
                // This handles the case where a dimension value has no matching rows in one
                // of the FULL OUTER JOIN branches (e.g., no detractors for is_reseller=true).
                let final_expr = if let Some(fill_value) = fill_nulls_map.get(metric_col) {
                    SqlExpr::FunctionCall {
                        function: "COALESCE".into(),
                        args: vec![max_expr, SqlExpr::Literal(fill_value.to_string())],
                    }
                } else {
                    max_expr
                };
                outer_select.push(SqlExpr::Alias {
                    expr: Box::new(final_expr),
                    alias: metric_col.clone(),
                });
            }
        }

        let outer_group_by: Vec<SqlExpr> = group_by_cols
            .iter()
            .map(|col| SqlExpr::ColumnRef {
                table_alias: reagg_alias.clone(),
                column_name: col.clone(),
            })
            .collect();

        Ok(SqlSelect {
            select_columns: outer_select,
            from: SqlFrom::Subquery {
                query: Box::new(inner),
                alias: reagg_alias,
            },
            joins: vec![],
            where_clause: None,
            group_by: outer_group_by,
            order_by: vec![],
            limit: None,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn convert_join_over_time_range<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    time_spine_table: &str,
    time_spine_column: &str,
    _time_spine_grain: TimeGrain,
    window: Option<&TimeWindow>,
    grain_to_date: Option<TimeGrain>,
    metric_time_column: &str,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    // The single parent is the ReadFromSource (or already-joined source).
    let parents = plan.parents(node_idx);
    let source_sql = convert_node(plan, parents[0], subquery_counter, graph)?;

    // Alias for the source side
    let source_alias = match &source_sql.from {
        SqlFrom::Table { alias, .. } => alias.clone(),
        SqlFrom::Subquery { alias, .. } => alias.clone(),
    };

    // Alias for the time spine table
    let time_spine_alias = format!("{time_spine_table}_spine");

    // Build ON condition based on window type:
    //
    // Window (trailing N days):
    //   source.metric_time <= time_spine.ds
    //   AND source.metric_time > time_spine.ds - INTERVAL 'N grain'
    //
    // Grain-to-date (e.g., month-to-date):
    //   source.metric_time <= time_spine.ds
    //   AND source.metric_time >= DATE_TRUNC('month', time_spine.ds)
    //
    // All-time (no window, no grain_to_date):
    //   source.metric_time <= time_spine.ds
    let on_expr = if let Some(w) = window {
        SqlExpr::Literal(format!(
            "{source_alias}.{metric_time_column} <= {time_spine_alias}.{time_spine_column} AND {source_alias}.{metric_time_column} > {time_spine_alias}.{time_spine_column} - INTERVAL '{count} {grain}'",
            count = w.count,
            grain = w.grain,
        ))
    } else if let Some(g) = grain_to_date {
        SqlExpr::Literal(format!(
            "{source_alias}.{metric_time_column} <= {time_spine_alias}.{time_spine_column} AND {source_alias}.{metric_time_column} >= DATE_TRUNC('{grain}', {time_spine_alias}.{time_spine_column})",
            grain = g,
        ))
    } else {
        // All-time: every source row is included up to and including the spine date
        SqlExpr::Literal(format!(
            "{source_alias}.{metric_time_column} <= {time_spine_alias}.{time_spine_column}"
        ))
    };

    // The time spine is the driving table (FROM); the source is INNERJOINed.
    // We project time_spine.ds AS metric_time__day plus all columns from source.
    let grain_suffix = "day"; // Always day-level output for now
    let metric_time_alias = format!("metric_time__{grain_suffix}");

    let join = SqlJoin {
        join_type: "INNER JOIN".into(),
        source: source_sql.from.clone(),
        on: on_expr,
    };

    let time_range_subq_alias = format!("time_range_subq_{subquery_counter}");
    *subquery_counter += 1;

    let inner_select = SqlSelect {
        select_columns: vec![
            // time_spine.ds AS metric_time__day
            SqlExpr::Alias {
                expr: Box::new(SqlExpr::ColumnRef {
                    table_alias: time_spine_alias.clone(),
                    column_name: time_spine_column.to_string(),
                }),
                alias: metric_time_alias.clone(),
            },
            // source.* (all source columns)
            SqlExpr::Literal(format!("{source_alias}.*")),
        ],
        from: SqlFrom::Table {
            table: time_spine_table.to_string(),
            alias: time_spine_alias,
        },
        joins: vec![join],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    };

    // Return a SELECT * from the time-range subquery so the Aggregate node above
    // can reference column names (including metric_time__day) uniformly.
    Ok(SqlSelect {
        select_columns: vec![SqlExpr::Literal("*".into())],
        from: SqlFrom::Subquery {
            query: Box::new(inner_select),
            alias: time_range_subq_alias,
        },
        joins: vec![],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}

fn convert_compute_metric<'a>(
    plan: &DataflowPlan,
    node_idx: NodeIndex,
    metric_name: &str,
    expr: &Option<String>,
    subquery_counter: &mut u32,
    graph: Option<&'a SemanticGraph<'a>>,
) -> Result<SqlSelect, ConvertError> {
    let parents = plan.parents(node_idx);
    let parent_sql = convert_node(plan, parents[0], subquery_counter, graph)?;

    // Fresh alias for wrapping the parent as a subquery
    let subq_alias = format!("compute_subq_{subquery_counter}");
    *subquery_counter += 1;

    // Infer group-by column names from parent's select_columns.
    // CombineAggregatedOutputs produces explicit Alias entries for COALESCE'd
    // group-by columns and metric columns. We treat any Alias whose name is
    // NOT the metric and doesn't start with "__" as a group-by column.
    // For Aggregate parents, group_by is populated with ColumnRef entries.
    let mut group_by_cols: Vec<String> = parent_sql
        .group_by
        .iter()
        .filter_map(|e| match e {
            SqlExpr::ColumnRef { column_name, .. } => Some(column_name.clone()),
            _ => None,
        })
        .collect();

    if group_by_cols.is_empty() {
        // Infer from select_columns when parent has no explicit GROUP BY
        // (e.g., ComputeMetric or CombineAggregatedOutputs).
        // ColumnRef entries are pass-through group-by columns.
        // Alias entries that aren't the metric itself are also group-by columns.
        group_by_cols = parent_sql
            .select_columns
            .iter()
            .filter_map(|e| match e {
                SqlExpr::ColumnRef { column_name, .. } => Some(column_name.clone()),
                SqlExpr::Alias { alias, .. }
                    if alias != metric_name && !alias.starts_with("__") =>
                {
                    Some(alias.clone())
                }
                _ => None,
            })
            .collect();
    }

    // Filter out metric input column names from group_by_cols.
    // If the parent has Alias entries for metric inputs (e.g., "bookings", "instant_bookings"),
    // they shouldn't be in group_by. We can detect them: they're columns referenced by the expr.
    if let Some(e) = expr {
        group_by_cols.retain(|col| !e.contains(col.as_str()));
    }

    // Build SELECT: group-by pass-throughs + metric expression AS metric_name
    let mut select_columns: Vec<SqlExpr> = Vec::new();

    for col in &group_by_cols {
        select_columns.push(SqlExpr::ColumnRef {
            table_alias: subq_alias.clone(),
            column_name: col.clone(),
        });
    }

    // The metric expression references column names available in parent subquery
    let metric_expr = match expr {
        Some(e) => SqlExpr::Literal(e.clone()),
        None => SqlExpr::Literal(metric_name.to_string()),
    };

    select_columns.push(SqlExpr::Alias {
        expr: Box::new(metric_expr),
        alias: metric_name.to_string(),
    });

    Ok(SqlSelect {
        select_columns,
        from: SqlFrom::Subquery {
            query: Box::new(parent_sql),
            alias: subq_alias,
        },
        joins: vec![],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mf_core::types::AggregationType;
    use mf_planning::dataflow::{DataflowNode, DataflowPlan, GroupByColumn, MeasureAggregation};

    fn build_test_plan() -> DataflowPlan {
        let mut plan = DataflowPlan::new();

        let read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });

        let agg = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "bookings".into(),
                agg_type: AggregationType::Sum,
                expr: "1".into(),
                alias: "bookings".into(),
                fill_nulls_with: None,
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

    #[test]
    fn test_convert_join_on_entities_standalone() {
        // Build a plan manually: left_read → join, right_read → join, join → agg
        let mut plan = DataflowPlan::new();

        let left_read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });
        let right_read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "listings_source".into(),
            table: "demo.dim_listings_latest".into(),
        });
        let join = plan.add_node(DataflowNode::JoinOnEntities {
            entity_name: "listing".into(),
            left_key: "listing_id".into(),
            right_key: "listing_id".into(),
            right_model_name: "listings_source".into(),
        });
        plan.add_edge(left_read, join);
        plan.add_edge(right_read, join);

        let agg = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("listing__country")],
            aggregations: vec![MeasureAggregation {
                measure_name: "bookings".into(),
                agg_type: AggregationType::Sum,
                expr: "1".into(),
                alias: "bookings".into(),
                fill_nulls_with: None,
            }],
        });
        plan.add_edge(join, agg);
        plan.set_sink(agg);

        // Without graph, right-side columns are not projected, but structure should work.
        // The sink is an Aggregate whose FROM is a subquery wrapping a join subquery.
        let sql = to_sql_standalone(&plan).unwrap();

        // Outer SELECT is aggregate, FROM is a subquery (the inner aggregate subquery)
        match &sql.from {
            SqlFrom::Subquery { query, alias } => {
                assert!(alias.starts_with("subq_"), "outer alias: {alias}");
                // Inner subquery should be FROM a join subquery
                match &query.from {
                    SqlFrom::Subquery {
                        query: join_q,
                        alias: join_alias,
                    } => {
                        assert!(
                            join_alias.starts_with("join_subq_"),
                            "join alias: {join_alias}"
                        );
                        // The join subquery should have a LEFT OUTER JOIN
                        assert_eq!(join_q.joins.len(), 1);
                        assert_eq!(join_q.joins[0].join_type, "LEFT OUTER JOIN");
                    }
                    SqlFrom::Table { .. } => panic!("expected join subquery, got table"),
                }
            }
            SqlFrom::Table { .. } => panic!("expected subquery, got table"),
        }
        assert_eq!(sql.group_by.len(), 1);
    }

    #[test]
    fn test_convert_where_filter_node_produces_where_clause() {
        // Build plan: ReadFromSource → WhereFilter → Aggregate
        let mut plan = DataflowPlan::new();

        let read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "orders_source".into(),
            table: "demo.fct_orders".into(),
        });

        let filter = plan.add_node(DataflowNode::WhereFilter {
            filters: vec![ResolvedFilterInfo {
                sql: "customer__status = 'active'".into(),
                required_columns: vec![("customer__status".into(), "status".into())],
            }],
        });
        plan.add_edge(read, filter);

        let agg = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "orders".into(),
                agg_type: AggregationType::Sum,
                expr: "1".into(),
                alias: "orders".into(),
                fill_nulls_with: None,
            }],
        });
        plan.add_edge(filter, agg);
        plan.set_sink(agg);

        let sql = to_sql_standalone(&plan).unwrap();
        let renderer = crate::render::renderer_for_dialect(mf_core::dialect::SqlDialect::DuckDB);
        let rendered = renderer.render(&sql);

        // Should contain WHERE clause
        assert!(
            rendered.contains("WHERE"),
            "should have WHERE clause: {rendered}"
        );
        assert!(
            rendered.contains("customer__status = 'active'"),
            "should have filter condition: {rendered}"
        );
        // Required column should be projected
        assert!(
            rendered.contains("status AS customer__status"),
            "should project filter column: {rendered}"
        );
        // Should still have aggregation
        assert!(
            rendered.contains("SUM"),
            "should have SUM aggregation: {rendered}"
        );
        assert!(
            rendered.contains("GROUP BY"),
            "should have GROUP BY: {rendered}"
        );
    }

    #[test]
    fn test_convert_where_filter_node_multiple_filters() {
        // Build plan: ReadFromSource → WhereFilter (2 filters) → Aggregate
        let mut plan = DataflowPlan::new();

        let read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "orders_source".into(),
            table: "demo.fct_orders".into(),
        });

        let filter = plan.add_node(DataflowNode::WhereFilter {
            filters: vec![
                ResolvedFilterInfo {
                    sql: "customer__status = 'active'".into(),
                    required_columns: vec![("customer__status".into(), "status".into())],
                },
                ResolvedFilterInfo {
                    sql: "customer__region = 'US'".into(),
                    required_columns: vec![("customer__region".into(), "region".into())],
                },
            ],
        });
        plan.add_edge(read, filter);

        let agg = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "orders".into(),
                agg_type: AggregationType::Sum,
                expr: "1".into(),
                alias: "orders".into(),
                fill_nulls_with: None,
            }],
        });
        plan.add_edge(filter, agg);
        plan.set_sink(agg);

        let sql = to_sql_standalone(&plan).unwrap();
        let renderer = crate::render::renderer_for_dialect(mf_core::dialect::SqlDialect::DuckDB);
        let rendered = renderer.render(&sql);

        // Both filters should be ANDed in the WHERE clause
        assert!(
            rendered.contains("customer__status = 'active'"),
            "should have first filter: {rendered}"
        );
        assert!(
            rendered.contains("customer__region = 'US'"),
            "should have second filter: {rendered}"
        );
        assert!(
            rendered.contains(" AND "),
            "filters should be ANDed: {rendered}"
        );
        // Both required columns should be projected
        assert!(
            rendered.contains("status AS customer__status"),
            "should project first filter column: {rendered}"
        );
        assert!(
            rendered.contains("region AS customer__region"),
            "should project second filter column: {rendered}"
        );
    }

    #[test]
    fn test_combine_compute_metric_parents_uses_full_outer_join() {
        // Regression test: when CombineAggregatedOutputs has ComputeMetric parents
        // (e.g., multi-metric query with derived metrics), the combine should use
        // FULL OUTER JOIN on shared group-by columns, not CROSS JOIN.
        let mut plan = DataflowPlan::new();

        // Build two independent metric subplans: read → agg → compute_metric
        let read1 = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "src".into(),
            table: "demo.fct".into(),
        });
        let agg1 = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "m1".into(),
                agg_type: AggregationType::Sum,
                expr: "val1".into(),
                alias: "m1".into(),
                fill_nulls_with: None,
            }],
        });
        plan.add_edge(read1, agg1);
        let compute1 = plan.add_node(DataflowNode::ComputeMetric {
            metric_name: "metric_a".to_string(),
            expr: Some("m1".to_string()),
        });
        plan.add_edge(agg1, compute1);

        let read2 = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "src".into(),
            table: "demo.fct".into(),
        });
        let agg2 = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "m2".into(),
                agg_type: AggregationType::Sum,
                expr: "val2".into(),
                alias: "m2".into(),
                fill_nulls_with: None,
            }],
        });
        plan.add_edge(read2, agg2);
        let compute2 = plan.add_node(DataflowNode::ComputeMetric {
            metric_name: "metric_b".to_string(),
            expr: Some("m2".to_string()),
        });
        plan.add_edge(agg2, compute2);

        // Combine the two ComputeMetric outputs
        let combine = plan.add_node(DataflowNode::CombineAggregatedOutputs);
        plan.add_edge(compute1, combine);
        plan.add_edge(compute2, combine);
        plan.set_sink(combine);

        let sql = to_sql_standalone(&plan).unwrap();
        let renderer = crate::render::renderer_for_dialect(mf_core::dialect::SqlDialect::DuckDB);
        let rendered = renderer.render(&sql);

        eprintln!("Rendered SQL:\n{rendered}");

        // Must use FULL OUTER JOIN (not CROSS JOIN) because both parents share metric_time__day
        assert!(
            rendered.contains("FULL OUTER JOIN"),
            "should use FULL OUTER JOIN when ComputeMetric parents share group-by columns: {rendered}"
        );
        assert!(
            !rendered.contains("CROSS JOIN"),
            "should NOT use CROSS JOIN: {rendered}"
        );
        // Should COALESCE the shared group-by column
        assert!(
            rendered.contains("COALESCE"),
            "should COALESCE shared group-by columns: {rendered}"
        );
        assert!(
            rendered.contains("metric_time__day"),
            "should include metric_time__day: {rendered}"
        );
        // Both metrics should appear
        assert!(
            rendered.contains("metric_a"),
            "should contain metric_a: {rendered}"
        );
        assert!(
            rendered.contains("metric_b"),
            "should contain metric_b: {rendered}"
        );
    }
}
