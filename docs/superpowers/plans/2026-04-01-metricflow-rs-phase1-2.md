# MetricFlow-RS Phase 1-2: Foundation + Simple Metrics

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working Rust pipeline that compiles a simple metric query (with group-by dimensions on the same semantic model) into DuckDB SQL, validated against Python MetricFlow's snapshot output.

**Architecture:** Layered crate workspace — `mf-core` (types) → `mf-manifest` (parsing + semantic graph) → `mf-planning` (dataflow DAG) → `mf-sql` (SQL generation). Each crate has a focused responsibility and clean interface. Rust enums replace Python's visitor pattern; petgraph replaces custom DAG classes.

**Tech Stack:** Rust 2024 edition, serde/serde_json/serde_yaml for manifest parsing, petgraph for DAGs, clap for CLI, insta for snapshot testing.

---

## File Structure

```
metricflow-rs/
├── Cargo.toml                          # workspace root
├── crates/
│   ├── mf-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # re-exports
│   │       ├── types.rs                # MetricType, AggregationType, TimeGrain, etc.
│   │       ├── spec.rs                 # QuerySpec, GroupBySpec, OrderBySpec
│   │       ├── manifest.rs             # SemanticManifest, SemanticModel, Metric, Measure, etc.
│   │       └── dialect.rs              # SqlDialect enum
│   ├── mf-manifest/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # public API: parse(), build_graph()
│   │       ├── parse.rs                # JSON deserialization into mf-core types
│   │       ├── graph.rs                # SemanticGraph construction
│   │       └── join_path.rs            # JoinPathResolver
│   ├── mf-planning/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # public API: plan_query()
│   │       ├── dataflow.rs             # DataflowNode enum, DataflowPlan struct
│   │       ├── resolve.rs              # query resolution (validate specs against graph)
│   │       └── builder.rs              # build dataflow plan for simple metrics
│   ├── mf-sql/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # public API: compile_query(), render()
│   │       ├── ast.rs                  # SqlNode, SqlSelectColumn, SqlExpr, SqlJoin
│   │       ├── convert.rs              # DataflowPlan → SqlNode
│   │       ├── render.rs               # SqlRenderer trait + DefaultRenderer
│   │       └── duckdb.rs               # DuckDbRenderer
│   └── mf-cli/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs                 # CLI binary
└── tests/
    └── fixtures/                       # extracted Python snapshot test fixtures
        └── simple_metric_duckdb.json
```

---

## Task 1: Initialize Workspace

**Files:**
- Create: `metricflow-rs/Cargo.toml`
- Create: `metricflow-rs/crates/mf-core/Cargo.toml`
- Create: `metricflow-rs/crates/mf-core/src/lib.rs`

- [ ] **Step 1: Create workspace root Cargo.toml**

```toml
# metricflow-rs/Cargo.toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
edition = "2024"
license = "Apache-2.0"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
petgraph = "0.7"
thiserror = "2"
clap = { version = "4", features = ["derive"] }
itertools = "0.14"
insta = { version = "1", features = ["yaml"] }
```

- [ ] **Step 2: Create mf-core crate**

```toml
# metricflow-rs/crates/mf-core/Cargo.toml
[package]
name = "mf-core"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
serde = { workspace = true }
thiserror = { workspace = true }
```

```rust
// metricflow-rs/crates/mf-core/src/lib.rs
pub mod types;
pub mod spec;
pub mod manifest;
pub mod dialect;
```

- [ ] **Step 3: Verify it compiles**

Run: `cd metricflow-rs && cargo check`
Expected: compiles with warnings about empty modules

- [ ] **Step 4: Commit**

```bash
git add metricflow-rs/
git commit -m "feat: initialize metricflow-rs workspace with mf-core crate"
```

---

## Task 2: Core Type Enums (`mf-core/src/types.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-core/src/types.rs`

- [ ] **Step 1: Write tests for type enums**

```rust
// metricflow-rs/crates/mf-core/src/types.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregation_type_from_str() {
        assert_eq!("sum".parse::<AggregationType>().unwrap(), AggregationType::Sum);
        assert_eq!("count_distinct".parse::<AggregationType>().unwrap(), AggregationType::CountDistinct);
        assert_eq!("SUM".parse::<AggregationType>().unwrap(), AggregationType::Sum);
    }

    #[test]
    fn test_time_grain_from_str() {
        assert_eq!("day".parse::<TimeGrain>().unwrap(), TimeGrain::Day);
        assert_eq!("MONTH".parse::<TimeGrain>().unwrap(), TimeGrain::Month);
    }

    #[test]
    fn test_time_grain_ordering() {
        assert!(TimeGrain::Day < TimeGrain::Week);
        assert!(TimeGrain::Week < TimeGrain::Month);
        assert!(TimeGrain::Month < TimeGrain::Quarter);
        assert!(TimeGrain::Quarter < TimeGrain::Year);
    }

    #[test]
    fn test_metric_type_from_str() {
        assert_eq!("simple".parse::<MetricKind>().unwrap(), MetricKind::Simple);
        assert_eq!("derived".parse::<MetricKind>().unwrap(), MetricKind::Derived);
        assert_eq!("cumulative".parse::<MetricKind>().unwrap(), MetricKind::Cumulative);
        assert_eq!("conversion".parse::<MetricKind>().unwrap(), MetricKind::Conversion);
        assert_eq!("ratio".parse::<MetricKind>().unwrap(), MetricKind::Ratio);
    }

    #[test]
    fn test_entity_type_from_str() {
        assert_eq!("primary".parse::<EntityType>().unwrap(), EntityType::Primary);
        assert_eq!("foreign".parse::<EntityType>().unwrap(), EntityType::Foreign);
        assert_eq!("unique".parse::<EntityType>().unwrap(), EntityType::Unique);
        assert_eq!("natural".parse::<EntityType>().unwrap(), EntityType::Natural);
    }

    #[test]
    fn test_dimension_type_from_str() {
        assert_eq!("categorical".parse::<DimensionType>().unwrap(), DimensionType::Categorical);
        assert_eq!("time".parse::<DimensionType>().unwrap(), DimensionType::Time);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-core`
Expected: compilation errors — types not defined yet

- [ ] **Step 3: Implement type enums**

```rust
// metricflow-rs/crates/mf-core/src/types.rs
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationType {
    Sum,
    Min,
    Max,
    CountDistinct,
    SumBoolean,
    Average,
    Percentile,
    Median,
    Count,
}

impl FromStr for AggregationType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sum" => Ok(Self::Sum),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "count_distinct" => Ok(Self::CountDistinct),
            "sum_boolean" => Ok(Self::SumBoolean),
            "average" => Ok(Self::Average),
            "percentile" => Ok(Self::Percentile),
            "median" => Ok(Self::Median),
            "count" => Ok(Self::Count),
            _ => Err(format!("unknown aggregation type: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeGrain {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl FromStr for TimeGrain {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "nanosecond" => Ok(Self::Nanosecond),
            "microsecond" => Ok(Self::Microsecond),
            "millisecond" => Ok(Self::Millisecond),
            "second" => Ok(Self::Second),
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "week" => Ok(Self::Week),
            "month" => Ok(Self::Month),
            "quarter" => Ok(Self::Quarter),
            "year" => Ok(Self::Year),
            _ => Err(format!("unknown time grain: {s}")),
        }
    }
}

impl fmt::Display for TimeGrain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Nanosecond => "nanosecond",
            Self::Microsecond => "microsecond",
            Self::Millisecond => "millisecond",
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Simple,
    Derived,
    Cumulative,
    Conversion,
    Ratio,
}

impl FromStr for MetricKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(Self::Simple),
            "derived" => Ok(Self::Derived),
            "cumulative" => Ok(Self::Cumulative),
            "conversion" => Ok(Self::Conversion),
            "ratio" => Ok(Self::Ratio),
            _ => Err(format!("unknown metric kind: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Primary,
    Foreign,
    Natural,
    Unique,
}

impl FromStr for EntityType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "primary" => Ok(Self::Primary),
            "foreign" => Ok(Self::Foreign),
            "natural" => Ok(Self::Natural),
            "unique" => Ok(Self::Unique),
            _ => Err(format!("unknown entity type: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DimensionType {
    Categorical,
    Time,
}

impl FromStr for DimensionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "categorical" => Ok(Self::Categorical),
            "time" => Ok(Self::Time),
            _ => Err(format!("unknown dimension type: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    LeftOuter,
    Inner,
    FullOuter,
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub count: u64,
    pub grain: TimeGrain,
}

// ... tests from Step 1 go here
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-core`
Expected: all 6 tests pass

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-core/src/types.rs
git commit -m "feat(mf-core): add core type enums with parsing"
```

---

## Task 3: Manifest Types (`mf-core/src/manifest.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-core/src/manifest.rs`

- [ ] **Step 1: Write tests for manifest types**

```rust
// at bottom of metricflow-rs/crates/mf-core/src/manifest.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    #[test]
    fn test_node_relation_fully_qualified() {
        let nr = NodeRelation {
            alias: "bookings".into(),
            schema_name: "demo".into(),
            database: Some("analytics_db".into()),
        };
        assert_eq!(nr.fully_qualified(), "analytics_db.demo.bookings");
    }

    #[test]
    fn test_node_relation_no_database() {
        let nr = NodeRelation {
            alias: "bookings".into(),
            schema_name: "demo".into(),
            database: None,
        };
        assert_eq!(nr.fully_qualified(), "demo.bookings");
    }

    #[test]
    fn test_measure_expr_defaults_to_name() {
        let m = Measure {
            name: "booking_count".into(),
            agg: AggregationType::Sum,
            expr: None,
            agg_time_dimension: None,
            non_additive_dimension: None,
            description: None,
            label: None,
        };
        assert_eq!(m.sql_expr(), "booking_count");
    }

    #[test]
    fn test_measure_expr_override() {
        let m = Measure {
            name: "booking_count".into(),
            agg: AggregationType::Sum,
            expr: Some("1".into()),
            agg_time_dimension: None,
            non_additive_dimension: None,
            description: None,
            label: None,
        };
        assert_eq!(m.sql_expr(), "1");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-core`
Expected: compilation errors — manifest types not defined

- [ ] **Step 3: Implement manifest types**

```rust
// metricflow-rs/crates/mf-core/src/manifest.rs
use serde::{Deserialize, Serialize};
use crate::types::*;

// --- Top-level manifest ---

#[derive(Debug, Clone, Deserialize)]
pub struct SemanticManifest {
    pub semantic_models: Vec<SemanticModel>,
    pub metrics: Vec<Metric>,
    pub project_configuration: ProjectConfiguration,
    #[serde(default)]
    pub saved_queries: Vec<SavedQuery>,
}

// --- Semantic Model ---

#[derive(Debug, Clone, Deserialize)]
pub struct SemanticModel {
    pub name: String,
    pub node_relation: NodeRelation,
    #[serde(default)]
    pub defaults: Option<SemanticModelDefaults>,
    pub primary_entity: Option<String>,
    #[serde(default)]
    pub entities: Vec<Entity>,
    #[serde(default)]
    pub measures: Vec<Measure>,
    #[serde(default)]
    pub dimensions: Vec<Dimension>,
    pub description: Option<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SemanticModelDefaults {
    pub agg_time_dimension: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeRelation {
    pub alias: String,
    pub schema_name: String,
    pub database: Option<String>,
}

impl NodeRelation {
    pub fn fully_qualified(&self) -> String {
        match &self.database {
            Some(db) => format!("{}.{}.{}", db, self.schema_name, self.alias),
            None => format!("{}.{}", self.schema_name, self.alias),
        }
    }
}

// --- Elements ---

#[derive(Debug, Clone, Deserialize)]
pub struct Measure {
    pub name: String,
    pub agg: AggregationType,
    pub expr: Option<String>,
    pub agg_time_dimension: Option<String>,
    pub non_additive_dimension: Option<NonAdditiveDimensionParameters>,
    pub description: Option<String>,
    pub label: Option<String>,
}

impl Measure {
    /// Returns the SQL expression for this measure. Defaults to the measure name.
    pub fn sql_expr(&self) -> &str {
        self.expr.as_deref().unwrap_or(&self.name)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NonAdditiveDimensionParameters {
    pub name: String,
    #[serde(default = "default_min_agg")]
    pub window_choice: AggregationType,
    #[serde(default)]
    pub window_groupings: Vec<String>,
}

fn default_min_agg() -> AggregationType {
    AggregationType::Min
}

#[derive(Debug, Clone, Deserialize)]
pub struct Dimension {
    pub name: String,
    #[serde(rename = "type")]
    pub dimension_type: DimensionType,
    #[serde(default)]
    pub is_partition: bool,
    pub type_params: Option<DimensionTypeParams>,
    pub expr: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
}

impl Dimension {
    /// Returns the SQL expression for this dimension. Defaults to the dimension name.
    pub fn sql_expr(&self) -> &str {
        self.expr.as_deref().unwrap_or(&self.name)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DimensionTypeParams {
    pub time_granularity: TimeGrain,
    pub validity_params: Option<DimensionValidityParams>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DimensionValidityParams {
    pub is_start: bool,
    pub is_end: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Entity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: EntityType,
    pub expr: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
}

impl Entity {
    /// Returns the SQL expression for this entity. Defaults to the entity name.
    pub fn sql_expr(&self) -> &str {
        self.expr.as_deref().unwrap_or(&self.name)
    }
}

// --- Metric ---

#[derive(Debug, Clone, Deserialize)]
pub struct Metric {
    pub name: String,
    #[serde(rename = "type")]
    pub metric_type: MetricKind,
    pub type_params: MetricTypeParams,
    pub filter: Option<WhereFilterIntersection>,
    pub description: Option<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricTypeParams {
    // Simple
    pub measure: Option<MetricInputMeasure>,
    // Ratio
    pub numerator: Option<MetricInput>,
    pub denominator: Option<MetricInput>,
    // Derived
    pub metrics: Option<Vec<MetricInput>>,
    pub expr: Option<String>,
    // Cumulative
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<TimeGrain>,
    // Conversion
    pub conversion_type_params: Option<ConversionTypeParams>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricInputMeasure {
    pub name: String,
    pub filter: Option<WhereFilterIntersection>,
    pub alias: Option<String>,
    #[serde(default)]
    pub join_to_timespine: bool,
    pub fill_nulls_with: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricInput {
    pub name: String,
    pub filter: Option<WhereFilterIntersection>,
    pub alias: Option<String>,
    pub offset_window: Option<MetricTimeWindow>,
    pub offset_to_grain: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricTimeWindow {
    pub count: i32,
    pub granularity: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConversionTypeParams {
    pub base_measure: Option<MetricInputMeasure>,
    pub conversion_measure: Option<MetricInputMeasure>,
    pub entity: String,
    #[serde(default = "default_conversion_rate")]
    pub calculation: String,
    pub window: Option<MetricTimeWindow>,
    pub constant_properties: Option<Vec<ConstantPropertyInput>>,
}

fn default_conversion_rate() -> String {
    "conversion_rate".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConstantPropertyInput {
    pub base_property: String,
    pub conversion_property: String,
}

// --- Filters ---

#[derive(Debug, Clone, Deserialize)]
pub struct WhereFilterIntersection {
    pub where_filters: Vec<WhereFilter>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WhereFilter {
    pub where_sql_template: String,
}

// --- Project Configuration ---

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfiguration {
    #[serde(default)]
    pub time_spine_table_configurations: Vec<TimeSpineTableConfiguration>,
    #[serde(default)]
    pub time_spines: Vec<TimeSpine>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimeSpineTableConfiguration {
    pub location: String,
    pub column_name: String,
    pub grain: TimeGrain,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimeSpine {
    pub node_relation: NodeRelation,
    pub primary_column: TimeSpinePrimaryColumn,
    #[serde(default)]
    pub custom_granularities: Vec<TimeSpineCustomGranularityColumn>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimeSpinePrimaryColumn {
    pub name: String,
    pub time_granularity: TimeGrain,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimeSpineCustomGranularityColumn {
    pub name: String,
    pub column_name: Option<String>,
}

// --- Saved Query (included for completeness, not used in Phase 1-2) ---

#[derive(Debug, Clone, Deserialize)]
pub struct SavedQuery {
    pub name: String,
    pub description: Option<String>,
    pub query_params: SavedQueryQueryParams,
    #[serde(default)]
    pub exports: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SavedQueryQueryParams {
    #[serde(default)]
    pub metrics: Vec<String>,
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(default)]
    pub order_by: Vec<String>,
    pub limit: Option<i32>,
}

// tests from Step 1 go here
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-core`
Expected: all tests pass (previous + new)

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-core/src/manifest.rs
git commit -m "feat(mf-core): add manifest types for semantic models, metrics, measures, dimensions, entities"
```

---

## Task 4: Query Spec & Dialect (`mf-core/src/spec.rs`, `mf-core/src/dialect.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-core/src/spec.rs`
- Create: `metricflow-rs/crates/mf-core/src/dialect.rs`

- [ ] **Step 1: Write tests**

```rust
// metricflow-rs/crates/mf-core/src/spec.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimeGrain;

    #[test]
    fn test_query_spec_builder() {
        let spec = QuerySpec {
            metrics: vec!["bookings".into()],
            group_by: vec![
                GroupBySpec::TimeDimension {
                    name: "metric_time".into(),
                    grain: TimeGrain::Day,
                    entity_path: vec![],
                },
            ],
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-core`
Expected: compilation errors

- [ ] **Step 3: Implement spec and dialect**

```rust
// metricflow-rs/crates/mf-core/src/spec.rs
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
                    format!("{}_{name}", entity_path.join("__"))
                }
            }
            GroupBySpec::TimeDimension { name, grain, entity_path } => {
                let base = if entity_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}_{name}", entity_path.join("__"))
                };
                format!("{base}__{grain}")
            }
            GroupBySpec::Entity { name, entity_path } => {
                if entity_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}_{name}", entity_path.join("__"))
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

// tests from Step 1 go here
```

```rust
// metricflow-rs/crates/mf-core/src/dialect.rs
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlDialect {
    DuckDB,
    BigQuery,
    Snowflake,
    Redshift,
    Postgres,
    Databricks,
    Trino,
}

impl fmt::Display for SqlDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::DuckDB => "DuckDB",
            Self::BigQuery => "BigQuery",
            Self::Snowflake => "Snowflake",
            Self::Redshift => "Redshift",
            Self::Postgres => "Postgres",
            Self::Databricks => "Databricks",
            Self::Trino => "Trino",
        };
        write!(f, "{s}")
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-core`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-core/src/spec.rs metricflow-rs/crates/mf-core/src/dialect.rs
git commit -m "feat(mf-core): add QuerySpec, GroupBySpec, SqlDialect types"
```

---

## Task 5: Manifest Parsing (`mf-manifest`)

**Files:**
- Create: `metricflow-rs/crates/mf-manifest/Cargo.toml`
- Create: `metricflow-rs/crates/mf-manifest/src/lib.rs`
- Create: `metricflow-rs/crates/mf-manifest/src/parse.rs`
- Create: `metricflow-rs/tests/fixtures/simple_manifest.json`

- [ ] **Step 1: Create mf-manifest crate**

```toml
# metricflow-rs/crates/mf-manifest/Cargo.toml
[package]
name = "mf-manifest"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
mf-core = { path = "../mf-core" }
serde = { workspace = true }
serde_json = { workspace = true }
serde_yaml = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
insta = { workspace = true }
```

```rust
// metricflow-rs/crates/mf-manifest/src/lib.rs
pub mod parse;
pub mod graph;
pub mod join_path;
```

- [ ] **Step 2: Create a minimal test fixture**

Extract a minimal manifest from the Python test suite. This represents a single semantic model with one measure, one time dimension, one categorical dimension, one entity, and one simple metric.

```json
// metricflow-rs/tests/fixtures/simple_manifest.json
{
  "semantic_models": [
    {
      "name": "bookings_source",
      "node_relation": {
        "alias": "fct_bookings",
        "schema_name": "demo",
        "database": null
      },
      "defaults": {
        "agg_time_dimension": "ds"
      },
      "primary_entity": "booking",
      "entities": [
        {
          "name": "booking",
          "type": "primary",
          "expr": null
        }
      ],
      "measures": [
        {
          "name": "bookings",
          "agg": "sum",
          "expr": "1",
          "agg_time_dimension": "ds"
        }
      ],
      "dimensions": [
        {
          "name": "ds",
          "type": "time",
          "type_params": {
            "time_granularity": "day"
          },
          "expr": null
        },
        {
          "name": "is_instant",
          "type": "categorical",
          "expr": null
        }
      ]
    }
  ],
  "metrics": [
    {
      "name": "bookings",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        }
      }
    }
  ],
  "project_configuration": {
    "time_spine_table_configurations": [],
    "time_spines": []
  }
}
```

- [ ] **Step 3: Write parsing test**

```rust
// metricflow-rs/crates/mf-manifest/src/parse.rs
use mf_core::manifest::SemanticManifest;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("failed to parse JSON manifest: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to parse YAML manifest: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

pub fn from_json(json: &str) -> Result<SemanticManifest, ParseError> {
    Ok(serde_json::from_str(json)?)
}

pub fn from_yaml(yaml: &str) -> Result<SemanticManifest, ParseError> {
    Ok(serde_yaml::from_str(yaml)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mf_core::types::*;

    #[test]
    fn test_parse_simple_manifest() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = from_json(json).expect("should parse");

        assert_eq!(manifest.semantic_models.len(), 1);
        assert_eq!(manifest.metrics.len(), 1);

        let model = &manifest.semantic_models[0];
        assert_eq!(model.name, "bookings_source");
        assert_eq!(model.node_relation.fully_qualified(), "demo.fct_bookings");
        assert_eq!(model.primary_entity.as_deref(), Some("booking"));

        assert_eq!(model.measures.len(), 1);
        assert_eq!(model.measures[0].name, "bookings");
        assert_eq!(model.measures[0].agg, AggregationType::Sum);
        assert_eq!(model.measures[0].sql_expr(), "1");

        assert_eq!(model.dimensions.len(), 2);
        assert_eq!(model.dimensions[0].name, "ds");
        assert_eq!(model.dimensions[0].dimension_type, DimensionType::Time);
        assert_eq!(model.dimensions[1].name, "is_instant");
        assert_eq!(model.dimensions[1].dimension_type, DimensionType::Categorical);

        assert_eq!(model.entities.len(), 1);
        assert_eq!(model.entities[0].name, "booking");
        assert_eq!(model.entities[0].entity_type, EntityType::Primary);

        let metric = &manifest.metrics[0];
        assert_eq!(metric.name, "bookings");
        assert_eq!(metric.metric_type, MetricKind::Simple);
        assert_eq!(metric.type_params.measure.as_ref().unwrap().name, "bookings");
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd metricflow-rs && cargo test -p mf-manifest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-manifest/ metricflow-rs/tests/
git commit -m "feat(mf-manifest): add JSON/YAML manifest parsing with test fixture"
```

---

## Task 6: Semantic Graph (`mf-manifest/src/graph.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-manifest/src/graph.rs`

- [ ] **Step 1: Write tests for graph construction**

```rust
// metricflow-rs/crates/mf-manifest/src/graph.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn test_build_graph_single_model() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // Should find the bookings metric
        assert!(graph.find_metric("bookings").is_some());

        // Should find the semantic model for the bookings measure
        let models = graph.models_for_measure("bookings");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].name, "bookings_source");
    }

    #[test]
    fn test_graph_find_dimension_on_same_model() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // is_instant dimension is on bookings_source model — no join needed
        let result = graph.find_dimension("is_instant", "bookings_source");
        assert!(result.is_some());
        let (model, dim) = result.unwrap();
        assert_eq!(model.name, "bookings_source");
        assert_eq!(dim.name, "is_instant");
    }

    #[test]
    fn test_graph_metric_not_found() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        assert!(graph.find_metric("nonexistent").is_none());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-manifest`
Expected: compilation errors

- [ ] **Step 3: Implement SemanticGraph**

```rust
// metricflow-rs/crates/mf-manifest/src/graph.rs
use std::collections::HashMap;
use mf_core::manifest::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("duplicate metric name: {0}")]
    DuplicateMetric(String),
    #[error("metric '{0}' references unknown measure '{1}'")]
    UnknownMeasure(String, String),
}

/// Index over a SemanticManifest for fast lookups.
/// In Phase 1-2 this is a flat index. Later phases add petgraph for join planning.
#[derive(Debug)]
pub struct SemanticGraph<'a> {
    manifest: &'a SemanticManifest,
    metrics_by_name: HashMap<&'a str, &'a Metric>,
    /// Maps measure name → list of semantic models containing that measure
    models_by_measure: HashMap<&'a str, Vec<&'a SemanticModel>>,
    /// Maps (model_name, dimension_name) → dimension ref
    dimensions_by_model: HashMap<(&'a str, &'a str), &'a Dimension>,
    /// Maps model_name → model ref
    models_by_name: HashMap<&'a str, &'a SemanticModel>,
}

impl<'a> SemanticGraph<'a> {
    pub fn build(manifest: &'a SemanticManifest) -> Result<Self, GraphError> {
        let mut metrics_by_name = HashMap::new();
        for metric in &manifest.metrics {
            if metrics_by_name.insert(metric.name.as_str(), metric).is_some() {
                return Err(GraphError::DuplicateMetric(metric.name.clone()));
            }
        }

        let mut models_by_measure: HashMap<&str, Vec<&SemanticModel>> = HashMap::new();
        let mut dimensions_by_model: HashMap<(&str, &str), &Dimension> = HashMap::new();
        let mut models_by_name = HashMap::new();

        for model in &manifest.semantic_models {
            models_by_name.insert(model.name.as_str(), model);
            for measure in &model.measures {
                models_by_measure
                    .entry(measure.name.as_str())
                    .or_default()
                    .push(model);
            }
            for dim in &model.dimensions {
                dimensions_by_model.insert((model.name.as_str(), dim.name.as_str()), dim);
            }
        }

        Ok(Self {
            manifest,
            metrics_by_name,
            models_by_measure,
            dimensions_by_model,
            models_by_name,
        })
    }

    pub fn find_metric(&self, name: &str) -> Option<&'a Metric> {
        self.metrics_by_name.get(name).copied()
    }

    pub fn models_for_measure(&self, measure_name: &str) -> Vec<&'a SemanticModel> {
        self.models_by_measure
            .get(measure_name)
            .cloned()
            .unwrap_or_default()
    }

    pub fn find_model(&self, name: &str) -> Option<&'a SemanticModel> {
        self.models_by_name.get(name).copied()
    }

    /// Find a dimension on a specific model. Returns (model, dimension) if found.
    pub fn find_dimension(
        &self,
        dim_name: &str,
        model_name: &str,
    ) -> Option<(&'a SemanticModel, &'a Dimension)> {
        let dim = self.dimensions_by_model.get(&(model_name, dim_name)).copied()?;
        let model = self.models_by_name.get(model_name).copied()?;
        Some((model, dim))
    }

    /// Find the agg_time_dimension for a measure on a model.
    pub fn agg_time_dimension(
        &self,
        measure_name: &str,
        model_name: &str,
    ) -> Option<&'a Dimension> {
        let model = self.models_by_name.get(model_name)?;
        let measure = model.measures.iter().find(|m| m.name == measure_name)?;

        // Use measure-level agg_time_dimension, fall back to model defaults
        let time_dim_name = measure
            .agg_time_dimension
            .as_deref()
            .or_else(|| model.defaults.as_ref()?.agg_time_dimension.as_deref())?;

        model.dimensions.iter().find(|d| d.name == time_dim_name)
    }

    pub fn manifest(&self) -> &'a SemanticManifest {
        self.manifest
    }
}

// tests from Step 1 go here
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-manifest`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-manifest/src/graph.rs
git commit -m "feat(mf-manifest): add SemanticGraph with metric/measure/dimension lookup"
```

---

## Task 7: Dataflow Nodes (`mf-planning/src/dataflow.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-planning/Cargo.toml`
- Create: `metricflow-rs/crates/mf-planning/src/lib.rs`
- Create: `metricflow-rs/crates/mf-planning/src/dataflow.rs`

- [ ] **Step 1: Create mf-planning crate**

```toml
# metricflow-rs/crates/mf-planning/Cargo.toml
[package]
name = "mf-planning"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
mf-core = { path = "../mf-core" }
mf-manifest = { path = "../mf-manifest" }
petgraph = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
insta = { workspace = true }
serde_json = { workspace = true }
```

```rust
// metricflow-rs/crates/mf-planning/src/lib.rs
pub mod dataflow;
pub mod resolve;
pub mod builder;
```

- [ ] **Step 2: Write test for dataflow plan construction**

```rust
// metricflow-rs/crates/mf-planning/src/dataflow.rs

#[cfg(test)]
mod tests {
    use super::*;
    use mf_core::types::*;

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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-planning`
Expected: compilation errors

- [ ] **Step 4: Implement DataflowNode and DataflowPlan**

```rust
// metricflow-rs/crates/mf-planning/src/dataflow.rs
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

// tests from Step 2 go here
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-planning`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add metricflow-rs/crates/mf-planning/
git commit -m "feat(mf-planning): add DataflowNode enum and DataflowPlan DAG"
```

---

## Task 8: Dataflow Plan Builder for Simple Metrics (`mf-planning/src/builder.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-planning/src/resolve.rs`
- Create: `metricflow-rs/crates/mf-planning/src/builder.rs`

- [ ] **Step 1: Write test for building a simple metric plan**

```rust
// metricflow-rs/crates/mf-planning/src/builder.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::DataflowNode;
    use mf_core::spec::*;
    use mf_core::types::*;
    use mf_manifest::parse;

    #[test]
    fn test_build_simple_metric_plan() {
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
            ],
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
            DataflowNode::Aggregate { group_by, aggregations } => {
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-planning`
Expected: compilation errors

- [ ] **Step 3: Implement resolve.rs**

```rust
// metricflow-rs/crates/mf-planning/src/resolve.rs
use mf_core::manifest::*;
use mf_core::spec::*;
use mf_core::types::*;
use mf_manifest::graph::SemanticGraph;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("unknown metric: '{0}'")]
    UnknownMetric(String),
    #[error("metric '{0}' is not a simple metric")]
    NotSimpleMetric(String),
    #[error("metric '{0}' has no measure defined")]
    NoMeasure(String),
    #[error("no semantic model found containing measure '{0}'")]
    NoModelForMeasure(String),
    #[error("dimension '{0}' not found on model '{1}'")]
    DimensionNotFound(String, String),
}

/// Resolved information needed to build a dataflow plan for a simple metric.
#[derive(Debug)]
pub struct ResolvedSimpleMetric<'a> {
    pub metric: &'a Metric,
    pub measure: &'a Measure,
    pub model: &'a SemanticModel,
    pub agg_time_dimension: Option<&'a Dimension>,
}

/// Resolve a simple metric: find its measure, source model, and time dimension.
pub fn resolve_simple_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedSimpleMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Simple {
        return Err(ResolveError::NotSimpleMetric(metric_name.into()));
    }

    let measure_ref = metric
        .type_params
        .measure
        .as_ref()
        .ok_or_else(|| ResolveError::NoMeasure(metric_name.into()))?;

    let models = graph.models_for_measure(&measure_ref.name);
    let model = models
        .first()
        .ok_or_else(|| ResolveError::NoModelForMeasure(measure_ref.name.clone()))?;

    let measure = model
        .measures
        .iter()
        .find(|m| m.name == measure_ref.name)
        .ok_or_else(|| ResolveError::NoModelForMeasure(measure_ref.name.clone()))?;

    let agg_time_dimension = graph.agg_time_dimension(&measure_ref.name, &model.name);

    Ok(ResolvedSimpleMetric {
        metric,
        measure,
        model,
        agg_time_dimension,
    })
}
```

- [ ] **Step 4: Implement builder.rs**

```rust
// metricflow-rs/crates/mf-planning/src/builder.rs
use crate::dataflow::*;
use crate::resolve::{self, ResolveError};
use mf_core::spec::*;
use mf_core::types::*;
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

// tests from Step 1 go here
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-planning`
Expected: all 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add metricflow-rs/crates/mf-planning/src/
git commit -m "feat(mf-planning): add dataflow plan builder for simple metrics"
```

---

## Task 9: SQL AST (`mf-sql/src/ast.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-sql/Cargo.toml`
- Create: `metricflow-rs/crates/mf-sql/src/lib.rs`
- Create: `metricflow-rs/crates/mf-sql/src/ast.rs`

- [ ] **Step 1: Create mf-sql crate**

```toml
# metricflow-rs/crates/mf-sql/Cargo.toml
[package]
name = "mf-sql"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
mf-core = { path = "../mf-core" }
mf-manifest = { path = "../mf-manifest" }
mf-planning = { path = "../mf-planning" }
thiserror = { workspace = true }
itertools = { workspace = true }

[dev-dependencies]
insta = { workspace = true }
serde_json = { workspace = true }
```

```rust
// metricflow-rs/crates/mf-sql/src/lib.rs
pub mod ast;
pub mod convert;
pub mod render;
pub mod duckdb;

use mf_core::dialect::SqlDialect;
use mf_core::manifest::SemanticManifest;
use mf_core::spec::QuerySpec;
use mf_manifest::graph::SemanticGraph;
use mf_planning::builder;

/// Top-level API: compile a query to SQL.
pub fn compile_query(
    manifest: &SemanticManifest,
    query: &QuerySpec,
    dialect: SqlDialect,
) -> Result<String, Box<dyn std::error::Error>> {
    let graph = SemanticGraph::build(manifest)?;
    let plan = builder::build_plan(&graph, query)?;
    let sql_node = convert::to_sql(&plan, &graph)?;
    let renderer = render::renderer_for_dialect(dialect);
    Ok(renderer.render(&sql_node))
}
```

- [ ] **Step 2: Write SQL AST types**

```rust
// metricflow-rs/crates/mf-sql/src/ast.rs

/// A SQL expression (column ref, function call, literal, etc.)
#[derive(Debug, Clone)]
pub enum SqlExpr {
    /// A column reference: `table_alias.column_name`
    ColumnRef {
        table_alias: String,
        column_name: String,
    },
    /// An aggregate function: `SUM(expr)`, `COUNT(DISTINCT expr)`, etc.
    AggregateFunction {
        function: String,
        arg: Box<SqlExpr>,
        distinct: bool,
    },
    /// A scalar function call: `DATE_TRUNC('day', expr)`
    FunctionCall {
        function: String,
        args: Vec<SqlExpr>,
    },
    /// A raw SQL literal or expression: `1`, `'day'`, etc.
    Literal(String),
    /// An aliased expression: `expr AS alias`
    Alias {
        expr: Box<SqlExpr>,
        alias: String,
    },
}

/// A SQL FROM source.
#[derive(Debug, Clone)]
pub enum SqlFrom {
    /// A table reference: `schema.table`
    Table {
        table: String,
        alias: String,
    },
    /// A subquery: `(SELECT ...) AS alias`
    Subquery {
        query: Box<SqlSelect>,
        alias: String,
    },
}

/// A SQL JOIN clause.
#[derive(Debug, Clone)]
pub struct SqlJoin {
    pub join_type: String, // "LEFT OUTER JOIN", "INNER JOIN", etc.
    pub source: SqlFrom,
    pub on: SqlExpr,
}

/// A SQL ORDER BY item.
#[derive(Debug, Clone)]
pub struct SqlOrderBy {
    pub expr: SqlExpr,
    pub descending: bool,
}

/// A complete SQL SELECT statement.
#[derive(Debug, Clone)]
pub struct SqlSelect {
    pub select_columns: Vec<SqlExpr>,
    pub from: SqlFrom,
    pub joins: Vec<SqlJoin>,
    pub where_clause: Option<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub order_by: Vec<SqlOrderBy>,
    pub limit: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_select_construction() {
        let select = SqlSelect {
            select_columns: vec![
                SqlExpr::Alias {
                    expr: Box::new(SqlExpr::AggregateFunction {
                        function: "SUM".into(),
                        arg: Box::new(SqlExpr::ColumnRef {
                            table_alias: "subq_0".into(),
                            column_name: "__bookings".into(),
                        }),
                        distinct: false,
                    }),
                    alias: "bookings".into(),
                },
            ],
            from: SqlFrom::Table {
                table: "demo.fct_bookings".into(),
                alias: "subq_0".into(),
            },
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };
        assert_eq!(select.select_columns.len(), 1);
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cd metricflow-rs && cargo test -p mf-sql`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add metricflow-rs/crates/mf-sql/
git commit -m "feat(mf-sql): add SQL AST types (SqlSelect, SqlExpr, SqlFrom, SqlJoin)"
```

---

## Task 10: Dataflow-to-SQL Converter (`mf-sql/src/convert.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-sql/src/convert.rs`

- [ ] **Step 1: Write test for conversion**

```rust
// metricflow-rs/crates/mf-sql/src/convert.rs

#[cfg(test)]
mod tests {
    use super::*;
    use mf_core::spec::*;
    use mf_core::types::*;
    use mf_manifest::parse;
    use mf_manifest::graph::SemanticGraph;
    use mf_planning::builder;

    fn build_test_plan() -> (DataflowPlan, SemanticGraph<'static>) {
        // We need a static manifest for the graph lifetime
        // Use a simpler approach: test conversion with a hand-built plan
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

        // Return plan without graph — converter doesn't need graph for simple case
        plan
    }

    #[test]
    fn test_convert_simple_aggregate() {
        let plan = build_test_plan();
        let sql = to_sql_standalone(&plan).unwrap();

        // Should be: SELECT SUM(subq.expr) AS alias, group_by FROM (SELECT ...) subq GROUP BY ...
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
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-sql`
Expected: compilation errors

- [ ] **Step 3: Implement converter**

```rust
// metricflow-rs/crates/mf-sql/src/convert.rs
use crate::ast::*;
use mf_core::types::*;
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
        DataflowNode::Aggregate { group_by, aggregations } => {
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
    subquery_counter: &mut u32,
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

// tests from Step 1 go here
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-sql`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-sql/src/convert.rs
git commit -m "feat(mf-sql): add dataflow-to-SQL converter for simple aggregations"
```

---

## Task 11: SQL Renderer (`mf-sql/src/render.rs` + `mf-sql/src/duckdb.rs`)

**Files:**
- Create: `metricflow-rs/crates/mf-sql/src/render.rs`
- Create: `metricflow-rs/crates/mf-sql/src/duckdb.rs`

- [ ] **Step 1: Write snapshot test for end-to-end SQL rendering**

```rust
// metricflow-rs/crates/mf-sql/src/render.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_render_simple_select() {
        let select = SqlSelect {
            select_columns: vec![
                SqlExpr::Alias {
                    expr: Box::new(SqlExpr::AggregateFunction {
                        function: "SUM".into(),
                        arg: Box::new(SqlExpr::ColumnRef {
                            table_alias: "subq_0".into(),
                            column_name: "__bookings".into(),
                        }),
                        distinct: false,
                    }),
                    alias: "bookings".into(),
                },
            ],
            from: SqlFrom::Subquery {
                query: Box::new(SqlSelect {
                    select_columns: vec![
                        SqlExpr::Alias {
                            expr: Box::new(SqlExpr::Literal("1".into())),
                            alias: "__bookings".into(),
                        },
                    ],
                    from: SqlFrom::Table {
                        table: "demo.fct_bookings".into(),
                        alias: "bookings_source_src".into(),
                    },
                    joins: vec![],
                    where_clause: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                }),
                alias: "subq_0".into(),
            },
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let renderer = DefaultRenderer;
        let sql = renderer.render(&select);
        assert!(sql.contains("SUM(subq_0.__bookings) AS bookings"));
        assert!(sql.contains("FROM ("));
        assert!(sql.contains("demo.fct_bookings"));
    }

    #[test]
    fn test_render_with_group_by() {
        let select = SqlSelect {
            select_columns: vec![
                SqlExpr::ColumnRef {
                    table_alias: "subq_0".into(),
                    column_name: "metric_time__day".into(),
                },
                SqlExpr::Alias {
                    expr: Box::new(SqlExpr::AggregateFunction {
                        function: "SUM".into(),
                        arg: Box::new(SqlExpr::ColumnRef {
                            table_alias: "subq_0".into(),
                            column_name: "__bookings".into(),
                        }),
                        distinct: false,
                    }),
                    alias: "bookings".into(),
                },
            ],
            from: SqlFrom::Table {
                table: "demo.fct_bookings".into(),
                alias: "subq_0".into(),
            },
            joins: vec![],
            where_clause: None,
            group_by: vec![
                SqlExpr::ColumnRef {
                    table_alias: "subq_0".into(),
                    column_name: "metric_time__day".into(),
                },
            ],
            order_by: vec![],
            limit: None,
        };

        let renderer = DefaultRenderer;
        let sql = renderer.render(&select);
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("metric_time__day"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-sql`
Expected: compilation errors

- [ ] **Step 3: Implement renderer**

```rust
// metricflow-rs/crates/mf-sql/src/render.rs
use crate::ast::*;
use mf_core::dialect::SqlDialect;

const INDENT: &str = "  ";

/// Trait for rendering SQL AST to a dialect-specific string.
pub trait SqlRenderer {
    fn render(&self, select: &SqlSelect) -> String;
    fn render_expr(&self, expr: &SqlExpr) -> String;
    fn quote_identifier(&self, name: &str) -> String {
        name.to_string() // default: no quoting
    }
}

/// Default ANSI SQL renderer.
pub struct DefaultRenderer;

impl SqlRenderer for DefaultRenderer {
    fn render(&self, select: &SqlSelect) -> String {
        let mut parts = Vec::new();

        // SELECT
        let cols: Vec<String> = select
            .select_columns
            .iter()
            .map(|c| self.render_expr(c))
            .collect();
        if cols.is_empty() {
            parts.push("SELECT\n  *".into());
        } else {
            let first = format!("SELECT\n{INDENT}{}", cols[0]);
            let rest: Vec<String> = cols[1..].iter().map(|c| format!("{INDENT}, {c}")).collect();
            let mut select_parts = vec![first];
            select_parts.extend(rest);
            parts.push(select_parts.join("\n"));
        }

        // FROM
        parts.push(self.render_from(&select.from));

        // JOINs
        for join in &select.joins {
            parts.push(self.render_join(join));
        }

        // WHERE
        if let Some(where_clause) = &select.where_clause {
            parts.push(format!("WHERE\n{INDENT}{}", self.render_expr(where_clause)));
        }

        // GROUP BY
        if !select.group_by.is_empty() {
            let group_cols: Vec<String> = select
                .group_by
                .iter()
                .map(|g| self.render_expr(g))
                .collect();
            let first = format!("GROUP BY\n{INDENT}{}", group_cols[0]);
            let rest: Vec<String> = group_cols[1..]
                .iter()
                .map(|c| format!("{INDENT}, {c}"))
                .collect();
            let mut group_parts = vec![first];
            group_parts.extend(rest);
            parts.push(group_parts.join("\n"));
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            let order_items: Vec<String> = select
                .order_by
                .iter()
                .map(|o| {
                    let expr = self.render_expr(&o.expr);
                    if o.descending {
                        format!("{expr} DESC")
                    } else {
                        expr
                    }
                })
                .collect();
            parts.push(format!("ORDER BY {}", order_items.join(", ")));
        }

        // LIMIT
        if let Some(limit) = select.limit {
            parts.push(format!("LIMIT {limit}"));
        }

        parts.join("\n")
    }

    fn render_expr(&self, expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::ColumnRef { table_alias, column_name } => {
                format!("{table_alias}.{column_name}")
            }
            SqlExpr::AggregateFunction { function, arg, distinct } => {
                let arg_str = self.render_expr(arg);
                if *distinct {
                    format!("{function}(DISTINCT {arg_str})")
                } else {
                    format!("{function}({arg_str})")
                }
            }
            SqlExpr::FunctionCall { function, args } => {
                let args_str: Vec<String> = args.iter().map(|a| self.render_expr(a)).collect();
                format!("{function}({})", args_str.join(", "))
            }
            SqlExpr::Literal(s) => s.clone(),
            SqlExpr::Alias { expr, alias } => {
                format!("{} AS {alias}", self.render_expr(expr))
            }
        }
    }
}

impl DefaultRenderer {
    fn render_from(&self, from: &SqlFrom) -> String {
        match from {
            SqlFrom::Table { table, alias } => {
                format!("FROM\n{INDENT}{table} {alias}")
            }
            SqlFrom::Subquery { query, alias } => {
                let inner = self.render(query);
                let indented = inner
                    .lines()
                    .map(|l| format!("{INDENT}{INDENT}{l}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("FROM (\n{indented}\n{INDENT}) {alias}")
            }
        }
    }

    fn render_join(&self, join: &SqlJoin) -> String {
        let source = match &join.source {
            SqlFrom::Table { table, alias } => format!("{INDENT}{table} {alias}"),
            SqlFrom::Subquery { query, alias } => {
                let inner = self.render(query);
                let indented = inner
                    .lines()
                    .map(|l| format!("{INDENT}{INDENT}{l}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{INDENT}(\n{indented}\n{INDENT}) {alias}")
            }
        };
        let on = self.render_expr(&join.on);
        format!("{}\n{source}\nON\n{INDENT}{on}", join.join_type)
    }
}

/// Return the appropriate renderer for a dialect.
pub fn renderer_for_dialect(dialect: SqlDialect) -> Box<dyn SqlRenderer> {
    match dialect {
        SqlDialect::DuckDB => Box::new(crate::duckdb::DuckDbRenderer),
        // All other dialects fall back to default for now
        _ => Box::new(DefaultRenderer),
    }
}

// tests from Step 1 go here
```

```rust
// metricflow-rs/crates/mf-sql/src/duckdb.rs
use crate::render::{DefaultRenderer, SqlRenderer};
use crate::ast::*;

/// DuckDB-specific SQL renderer.
/// Inherits most behavior from DefaultRenderer, overrides DuckDB-specific functions.
pub struct DuckDbRenderer;

impl SqlRenderer for DuckDbRenderer {
    fn render(&self, select: &SqlSelect) -> String {
        // Delegate to default renderer — DuckDB differences are in expression rendering
        DefaultRenderer.render(select)
    }

    fn render_expr(&self, expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::FunctionCall { function, args } if function == "DATE_TRUNC" => {
                // DuckDB: DATE_TRUNC('grain', expr) — same as ANSI, but ensure lowercase grain
                let args_str: Vec<String> = args.iter().map(|a| self.render_expr(a)).collect();
                format!("DATE_TRUNC({})", args_str.join(", "))
            }
            // All other expressions use default rendering
            _ => DefaultRenderer.render_expr(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_renders_date_trunc() {
        let expr = SqlExpr::FunctionCall {
            function: "DATE_TRUNC".into(),
            args: vec![
                SqlExpr::Literal("'day'".into()),
                SqlExpr::ColumnRef {
                    table_alias: "src".into(),
                    column_name: "ds".into(),
                },
            ],
        };
        let renderer = DuckDbRenderer;
        assert_eq!(renderer.render_expr(&expr), "DATE_TRUNC('day', src.ds)");
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd metricflow-rs && cargo test -p mf-sql`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-sql/src/render.rs metricflow-rs/crates/mf-sql/src/duckdb.rs
git commit -m "feat(mf-sql): add SQL renderer with DuckDB dialect support"
```

---

## Task 12: End-to-End Integration Test

**Files:**
- Create: `metricflow-rs/tests/integration.rs`

- [ ] **Step 1: Write end-to-end test**

```rust
// metricflow-rs/tests/integration.rs
use mf_core::dialect::SqlDialect;
use mf_core::spec::*;
use mf_core::types::*;

#[test]
fn test_end_to_end_simple_metric_duckdb() {
    let manifest_json = include_str!("fixtures/simple_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

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

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    // The SQL should contain key elements
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("__bookings"),
        "should have internal measure alias: {sql}"
    );
    assert!(
        sql.contains("bookings") && sql.contains("AS"),
        "should have final alias: {sql}"
    );
    assert!(
        sql.contains("metric_time__day"),
        "should have time dimension in GROUP BY: {sql}"
    );
    assert!(
        sql.contains("demo.fct_bookings"),
        "should reference source table: {sql}"
    );
    assert!(sql.contains("GROUP BY"), "should have GROUP BY: {sql}");

    // Print for manual inspection
    eprintln!("Generated SQL:\n{sql}");
}

#[test]
fn test_end_to_end_with_categorical_dimension() {
    let manifest_json = include_str!("fixtures/simple_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

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
        limit: Some(10),
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    assert!(sql.contains("is_instant"), "should include dimension: {sql}");
    assert!(sql.contains("LIMIT 10"), "should have LIMIT: {sql}");

    eprintln!("Generated SQL:\n{sql}");
}

#[test]
fn test_end_to_end_unknown_metric_error() {
    let manifest_json = include_str!("fixtures/simple_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nonexistent".into()],
        group_by: vec![],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let result = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("nonexistent"),
        "error should mention metric name: {err}"
    );
}
```

- [ ] **Step 2: Run the integration tests**

Run: `cd metricflow-rs && cargo test --test integration`
Expected: all 3 tests pass. The `eprintln!` output shows the generated SQL for manual inspection.

- [ ] **Step 3: Commit**

```bash
git add metricflow-rs/tests/integration.rs
git commit -m "test: add end-to-end integration tests for simple metric compilation"
```

---

## Task 13: CLI Binary (`mf-cli`)

**Files:**
- Create: `metricflow-rs/crates/mf-cli/Cargo.toml`
- Create: `metricflow-rs/crates/mf-cli/src/main.rs`

- [ ] **Step 1: Create mf-cli crate**

```toml
# metricflow-rs/crates/mf-cli/Cargo.toml
[package]
name = "mf-cli"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[[bin]]
name = "mf"
path = "src/main.rs"

[dependencies]
mf-core = { path = "../mf-core" }
mf-sql = { path = "../mf-sql" }
clap = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Implement CLI**

```rust
// metricflow-rs/crates/mf-cli/src/main.rs
use clap::{Parser, ValueEnum};
use mf_core::dialect::SqlDialect;
use mf_core::spec::*;
use mf_core::types::TimeGrain;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mf", about = "MetricFlow query compiler")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Compile a metric query to SQL
    Query {
        /// Path to semantic_manifest.json
        #[arg(long)]
        manifest: PathBuf,

        /// Metric names (comma-separated)
        #[arg(long, value_delimiter = ',')]
        metrics: Vec<String>,

        /// Group-by dimensions (comma-separated, e.g., "Dimension('region'),TimeDimension('metric_time', 'day')")
        #[arg(long, value_delimiter = ',')]
        group_by: Vec<String>,

        /// Time grain
        #[arg(long)]
        grain: Option<String>,

        /// SQL dialect
        #[arg(long, default_value = "duckdb")]
        dialect: DialectArg,

        /// Row limit
        #[arg(long)]
        limit: Option<u64>,
    },
}

#[derive(Clone, ValueEnum)]
enum DialectArg {
    Duckdb,
    Bigquery,
    Snowflake,
    Redshift,
    Postgres,
    Databricks,
    Trino,
}

impl From<DialectArg> for SqlDialect {
    fn from(d: DialectArg) -> Self {
        match d {
            DialectArg::Duckdb => SqlDialect::DuckDB,
            DialectArg::Bigquery => SqlDialect::BigQuery,
            DialectArg::Snowflake => SqlDialect::Snowflake,
            DialectArg::Redshift => SqlDialect::Redshift,
            DialectArg::Postgres => SqlDialect::Postgres,
            DialectArg::Databricks => SqlDialect::Databricks,
            DialectArg::Trino => SqlDialect::Trino,
        }
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            manifest,
            metrics,
            group_by,
            grain,
            dialect,
            limit,
        } => {
            let manifest_json = std::fs::read_to_string(&manifest).unwrap_or_else(|e| {
                eprintln!("Error reading manifest {}: {e}", manifest.display());
                std::process::exit(1);
            });

            let manifest: mf_core::manifest::SemanticManifest =
                serde_json::from_str(&manifest_json).unwrap_or_else(|e| {
                    eprintln!("Error parsing manifest: {e}");
                    std::process::exit(1);
                });

            // Parse group-by specs (simplified: just dimension names for now)
            let group_by_specs: Vec<GroupBySpec> = group_by
                .iter()
                .map(|g| {
                    // Simple heuristic: if grain is provided, treat first group-by as time dimension
                    if let Some(ref gr) = grain {
                        if g.contains("metric_time") || g.contains("time") {
                            return GroupBySpec::TimeDimension {
                                name: g.clone(),
                                grain: gr.parse().unwrap_or(TimeGrain::Day),
                                entity_path: vec![],
                            };
                        }
                    }
                    GroupBySpec::Dimension {
                        name: g.clone(),
                        entity_path: vec![],
                    }
                })
                .collect();

            let query = QuerySpec {
                metrics,
                group_by: group_by_specs,
                where_clauses: vec![],
                order_by: vec![],
                limit,
            };

            match mf_sql::compile_query(&manifest, &query, dialect.into()) {
                Ok(sql) => println!("{sql}"),
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}
```

- [ ] **Step 3: Verify it builds**

Run: `cd metricflow-rs && cargo build -p mf-cli`
Expected: compiles successfully

- [ ] **Step 4: Test the CLI manually**

Run: `cd metricflow-rs && cargo run -p mf-cli -- query --manifest tests/fixtures/simple_manifest.json --metrics bookings --group-by metric_time --grain day --dialect duckdb`
Expected: prints generated SQL to stdout

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-cli/
git commit -m "feat(mf-cli): add CLI binary for standalone metric query compilation"
```

---

## Task 14: Extract Python Snapshot Fixtures

**Files:**
- Create: `metricflow-rs/scripts/extract_fixtures.py`

This task creates a Python script that extracts test fixtures from the Python MetricFlow test suite for use as validation oracles.

- [ ] **Step 1: Write the extraction script**

```python
#!/usr/bin/env python3
"""Extract test fixtures from Python MetricFlow snapshot tests.

Reads snapshot SQL files and their corresponding test inputs,
outputs JSON fixtures for the Rust test suite.

Usage:
    uv run python scripts/extract_fixtures.py --metricflow-root /path/to/metricflow --output tests/fixtures/
"""
import argparse
import json
import re
from pathlib import Path


def find_snapshot_sql_files(metricflow_root: Path) -> list[Path]:
    """Find all SQL snapshot files."""
    snapshot_dir = metricflow_root / "tests_metricflow" / "snapshots"
    return sorted(snapshot_dir.rglob("*.sql"))


def extract_simple_metric_fixtures(metricflow_root: Path, output_dir: Path) -> None:
    """Extract simple metric test cases.

    For Phase 1-2, focus on test_dfs_planner and test_dataflow_plan_builder
    snapshots that test simple metrics.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find SQL snapshots related to simple metric tests
    snapshot_dir = metricflow_root / "tests_metricflow" / "snapshots"

    simple_patterns = [
        "test_simple_plan",
        "test_simple_query",
    ]

    found = 0
    for sql_file in snapshot_dir.rglob("*.sql"):
        for pattern in simple_patterns:
            if pattern in sql_file.name:
                fixture = {
                    "source_file": str(sql_file.relative_to(metricflow_root)),
                    "sql": sql_file.read_text().strip(),
                }
                out_name = sql_file.stem + ".json"
                out_path = output_dir / out_name
                out_path.write_text(json.dumps(fixture, indent=2))
                found += 1
                print(f"  Extracted: {out_name}")

    print(f"\nExtracted {found} fixtures to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Extract MetricFlow test fixtures")
    parser.add_argument(
        "--metricflow-root",
        type=Path,
        default=Path(__file__).parent.parent.parent / "metricflow",
        help="Path to Python MetricFlow repo root",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "tests" / "fixtures" / "snapshots",
        help="Output directory for JSON fixtures",
    )
    args = parser.parse_args()
    extract_simple_metric_fixtures(args.metricflow_root, args.output)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the script**

Run: `cd metricflow-rs && uv run python scripts/extract_fixtures.py --metricflow-root /Users/bper/dev/metricflow`
Expected: prints extracted fixture count, creates JSON files in `tests/fixtures/snapshots/`

- [ ] **Step 3: Commit**

```bash
git add metricflow-rs/scripts/ metricflow-rs/tests/fixtures/snapshots/
git commit -m "feat: add Python snapshot fixture extraction script"
```

---

## Task 15: Run All Tests, Final Verification

- [ ] **Step 1: Run full test suite**

Run: `cd metricflow-rs && cargo test --all`
Expected: all tests pass across all crates

- [ ] **Step 2: Run clippy**

Run: `cd metricflow-rs && cargo clippy --all -- -D warnings`
Expected: no warnings

- [ ] **Step 3: Run fmt check**

Run: `cd metricflow-rs && cargo fmt --all -- --check`
Expected: no formatting issues

- [ ] **Step 4: Commit any fixes**

If clippy or fmt found issues, fix them:
```bash
cd metricflow-rs && cargo fmt --all
git add -A && git commit -m "style: fix clippy warnings and formatting"
```

- [ ] **Step 5: Final commit summarizing Phase 1-2**

```bash
git log --oneline --since="today" | head -20
```

Review the commit history to verify all tasks were completed.

---

## What This Plan Produces

At the end of Phase 1-2, you have:

1. **A working Rust crate** that compiles `(manifest + query_spec + dialect) → SQL string`
2. **Validated against Python** via extracted snapshot fixtures
3. **A standalone CLI** for testing queries outside Fusion
4. **Clean crate boundaries** ready for Phase 3+ (joins, derived metrics, etc.)

## What Comes Next (Future Plans)

- **Phase 3 plan:** Entity join path planning — multi-hop joins to reach dimensions on other semantic models
- **Phase 4 plan:** Derived + cumulative metrics
- **Phase 5 plan:** Conversion + offset metrics
- **Phase 6 plan:** All SQL dialect renderers
- **Phase 7 plan:** Fusion integration (replace subprocess call)
- **Phase 8 plan:** SQL optimization passes + performance
