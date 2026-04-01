use mf_core::types::*;
use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};

/// A node in the dataflow plan DAG.
#[derive(Debug, Clone)]
pub enum DataflowNode {
    /// Read columns from a source table (semantic model).
    ReadFromSource { model_name: String, table: String },
    /// Join two sources on entity keys (always LEFT OUTER join in MetricFlow convention).
    /// The left source is the measure model; the right source provides dimensions.
    JoinOnEntities {
        /// The entity name used for the join (e.g. "listing")
        entity_name: String,
        /// SQL expression on the left side (e.g. "listing_id")
        left_key: String,
        /// SQL expression on the right side (e.g. "listing_id")
        right_key: String,
        /// The right-side model name (for column qualification)
        right_model_name: String,
    },
    /// Aggregate measures with GROUP BY.
    Aggregate {
        group_by: Vec<GroupByColumn>,
        aggregations: Vec<MeasureAggregation>,
    },
    /// Compute metric expression (e.g., for derived metrics).
    ComputeMetric {
        metric_name: String,
        expr: Option<String>,
    },
    /// Select/rename columns.
    SelectColumns { columns: Vec<ColumnSelect> },
    /// Apply a WHERE filter.
    WhereFilter { sql: String },
    /// ORDER BY.
    OrderBy {
        specs: Vec<(String, bool)>, // (column, descending)
    },
    /// LIMIT.
    Limit { count: u64 },
    /// Combine multiple aggregated metric outputs via FULL OUTER JOIN on shared dimensions.
    CombineAggregatedOutputs,
    /// Join source data against a time spine for cumulative metric computation.
    JoinOverTimeRange {
        time_spine_table: String,
        time_spine_column: String,
        time_spine_grain: TimeGrain,
        window: Option<TimeWindow>,
        grain_to_date: Option<TimeGrain>,
        metric_time_column: String,
    },
}

#[derive(Debug, Clone)]
pub struct MeasureAggregation {
    pub measure_name: String,
    pub agg_type: AggregationType,
    pub expr: String,
    pub alias: String,
}

/// A group-by column with both the output alias and the SQL expression.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByColumn {
    /// Output column name, e.g. "metric_time__day" or "is_instant"
    pub alias: String,
    /// SQL expression to compute this column, e.g. "DATE_TRUNC('day', close_month)" or "is_instant"
    pub expr: String,
}

impl GroupByColumn {
    pub fn simple(name: impl Into<String>) -> Self {
        let n = name.into();
        Self {
            alias: n.clone(),
            expr: n,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSelect {
    pub input_name: String,
    pub output_name: String,
    pub expr: Option<String>,
}

/// DAG-based dataflow plan. Edges point from parent (input) to child (consumer).
#[derive(Debug)]
pub struct DataflowPlan {
    dag: DiGraph<DataflowNode, ()>,
    sink: Option<NodeIndex>,
}

impl DataflowPlan {
    pub fn new() -> Self {
        Self {
            dag: DiGraph::new(),
            sink: None,
        }
    }

    pub fn add_node(&mut self, node: DataflowNode) -> NodeIndex {
        self.dag.add_node(node)
    }

    /// Add edge from parent (input) to child (consumer).
    pub fn add_edge(&mut self, parent: NodeIndex, child: NodeIndex) {
        self.dag.add_edge(parent, child, ());
    }

    pub fn set_sink(&mut self, node: NodeIndex) {
        self.sink = Some(node);
    }

    pub fn sink(&self) -> Option<NodeIndex> {
        self.sink
    }

    pub fn node(&self, idx: NodeIndex) -> &DataflowNode {
        &self.dag[idx]
    }

    pub fn node_count(&self) -> usize {
        self.dag.node_count()
    }

    /// Get parent (input) nodes of a given node.
    pub fn parents(&self, node: NodeIndex) -> Vec<NodeIndex> {
        self.dag
            .neighbors_directed(node, Direction::Incoming)
            .collect()
    }
}

impl Default for DataflowPlan {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_simple_plan_structure() {
        // Build a plan by hand to test the DAG structure
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
            }],
        });
        plan.add_edge(read, agg);
        plan.set_sink(agg);

        assert_eq!(plan.node_count(), 2);
        assert_eq!(plan.sink(), Some(agg));
        // The read node is a parent of the agg node
        let parents = plan.parents(agg);
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0], read);
    }

    #[test]
    fn test_join_on_entities_node() {
        let mut plan = DataflowPlan::new();

        let left = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });
        let right = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "listings_source".into(),
            table: "demo.dim_listings_latest".into(),
        });
        let join = plan.add_node(DataflowNode::JoinOnEntities {
            entity_name: "listing".into(),
            left_key: "listing_id".into(),
            right_key: "listing_id".into(),
            right_model_name: "listings_source".into(),
        });
        plan.add_edge(left, join);
        plan.add_edge(right, join);
        plan.set_sink(join);

        assert_eq!(plan.node_count(), 3);
        let parents = plan.parents(join);
        assert_eq!(parents.len(), 2);

        match plan.node(join) {
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
    }

    #[test]
    fn test_combine_aggregated_outputs_node() {
        let mut plan = DataflowPlan::new();
        let read1 = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });
        let agg1 = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "bookings".into(),
                agg_type: AggregationType::Sum,
                expr: "1".into(),
                alias: "bookings".into(),
            }],
        });
        plan.add_edge(read1, agg1);
        let read2 = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });
        let agg2 = plan.add_node(DataflowNode::Aggregate {
            group_by: vec![GroupByColumn::simple("metric_time__day")],
            aggregations: vec![MeasureAggregation {
                measure_name: "instant_bookings".into(),
                agg_type: AggregationType::Sum,
                expr: "is_instant".into(),
                alias: "instant_bookings".into(),
            }],
        });
        plan.add_edge(read2, agg2);
        let combine = plan.add_node(DataflowNode::CombineAggregatedOutputs);
        plan.add_edge(agg1, combine);
        plan.add_edge(agg2, combine);
        plan.set_sink(combine);
        assert_eq!(plan.node_count(), 5);
        assert_eq!(plan.parents(combine).len(), 2);
    }

    #[test]
    fn test_join_over_time_range_node() {
        let mut plan = DataflowPlan::new();
        let read = plan.add_node(DataflowNode::ReadFromSource {
            model_name: "bookings_source".into(),
            table: "demo.fct_bookings".into(),
        });
        let join_time = plan.add_node(DataflowNode::JoinOverTimeRange {
            time_spine_table: "mf_time_spine".into(),
            time_spine_column: "ds".into(),
            time_spine_grain: TimeGrain::Day,
            window: Some(TimeWindow {
                count: 7,
                grain: TimeGrain::Day,
            }),
            grain_to_date: None,
            metric_time_column: "ds".into(),
        });
        plan.add_edge(read, join_time);
        plan.set_sink(join_time);
        assert_eq!(plan.node_count(), 2);
        match plan.node(join_time) {
            DataflowNode::JoinOverTimeRange {
                window,
                grain_to_date,
                ..
            } => {
                assert!(window.is_some());
                assert!(grain_to_date.is_none());
            }
            other => panic!("expected JoinOverTimeRange, got {other:?}"),
        }
    }
}
