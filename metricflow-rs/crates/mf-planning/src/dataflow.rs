use mf_core::types::*;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::Direction;

/// A node in the dataflow plan DAG.
#[derive(Debug, Clone)]
pub enum DataflowNode {
    /// Read columns from a source table (semantic model).
    ReadFromSource {
        model_name: String,
        table: String,
    },
    /// Aggregate measures with GROUP BY.
    Aggregate {
        group_by: Vec<String>,
        aggregations: Vec<MeasureAggregation>,
    },
    /// Compute metric expression (e.g., for derived metrics).
    ComputeMetric {
        metric_name: String,
        expr: Option<String>,
    },
    /// Select/rename columns.
    SelectColumns {
        columns: Vec<ColumnSelect>,
    },
    /// Apply a WHERE filter.
    WhereFilter {
        sql: String,
    },
    /// ORDER BY.
    OrderBy {
        specs: Vec<(String, bool)>, // (column, descending)
    },
    /// LIMIT.
    Limit {
        count: u64,
    },
}

#[derive(Debug, Clone)]
pub struct MeasureAggregation {
    pub measure_name: String,
    pub agg_type: AggregationType,
    pub expr: String,
    pub alias: String,
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

        assert_eq!(plan.node_count(), 2);
        assert_eq!(plan.sink(), Some(agg));
        // The read node is a parent of the agg node
        let parents = plan.parents(agg);
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0], read);
    }
}
