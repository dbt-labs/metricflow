use crate::types::TimeGrain;

#[derive(Debug, Clone)]
pub struct QuerySpec {
    pub metrics: Vec<String>,
    pub group_by: Vec<GroupBySpec>,
    pub where_clauses: Vec<String>,
    pub order_by: Vec<OrderBySpec>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupBySpec {
    Dimension {
        name: String,
        entity_path: Vec<String>,
    },
    TimeDimension {
        name: String,
        grain: TimeGrain,
        entity_path: Vec<String>,
    },
    Entity {
        name: String,
        entity_path: Vec<String>,
    },
}

impl GroupBySpec {
    /// Returns the output column name using MetricFlow's `entity__name` convention.
    /// Time dimensions include the grain: `metric_time__day`.
    pub fn column_name(&self) -> String {
        match self {
            GroupBySpec::Dimension { name, entity_path } => {
                if entity_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}__{name}", entity_path.join("__"))
                }
            }
            GroupBySpec::TimeDimension {
                name,
                grain,
                entity_path,
            } => {
                let base = if entity_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}__{name}", entity_path.join("__"))
                };
                format!("{base}__{grain}")
            }
            GroupBySpec::Entity { name, entity_path } => {
                if entity_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}__{name}", entity_path.join("__"))
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBySpec {
    pub column: GroupBySpec,
    pub descending: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimeGrain;

    #[test]
    fn test_query_spec_builder() {
        let spec = QuerySpec {
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
        assert_eq!(spec.metrics.len(), 1);
        assert_eq!(spec.group_by.len(), 1);
    }

    #[test]
    fn test_group_by_spec_column_name() {
        let dim = GroupBySpec::Dimension {
            name: "country".into(),
            entity_path: vec!["user".into()],
        };
        assert_eq!(dim.column_name(), "user__country");

        let local_dim = GroupBySpec::Dimension {
            name: "is_instant".into(),
            entity_path: vec![],
        };
        assert_eq!(local_dim.column_name(), "is_instant");
    }

    #[test]
    fn test_time_dimension_column_name() {
        let td = GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        };
        assert_eq!(td.column_name(), "metric_time__day");
    }
}
