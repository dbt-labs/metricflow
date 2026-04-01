use crate::types::*;
use serde::Deserialize;

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
    // Simple (older format: measure field set directly)
    pub measure: Option<MetricInputMeasure>,
    // Simple (newer format: measure info inlined as aggregation params)
    pub metric_aggregation_params: Option<MetricAggregationParams>,
    // Ratio
    pub numerator: Option<MetricInput>,
    pub denominator: Option<MetricInput>,
    // Derived
    pub metrics: Option<Vec<MetricInput>>,
    pub expr: Option<String>,
    // Cumulative (older format: window/grain_to_date at top level)
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<TimeGrain>,
    // Cumulative (newer format: cumulative_type_params)
    pub cumulative_type_params: Option<CumulativeTypeParams>,
    // Conversion
    pub conversion_type_params: Option<ConversionTypeParams>,
    // Null filling: wrap metric output in COALESCE(metric, value)
    pub fill_nulls_with: Option<i64>,
    #[serde(default)]
    pub join_to_timespine: bool,
    #[serde(default)]
    pub is_private: bool,
}

/// Inlined measure info for simple metrics (newer manifest format).
/// Replaces the older `measure` field that references a measure by name.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricAggregationParams {
    /// The semantic model name containing this measure
    pub semantic_model: String,
    pub agg: AggregationType,
    pub expr: Option<String>,
    pub agg_time_dimension: Option<String>,
    pub non_additive_dimension: Option<NonAdditiveDimensionParameters>,
}

/// Cumulative metric parameters (newer manifest format).
#[derive(Debug, Clone, Deserialize)]
pub struct CumulativeTypeParams {
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<TimeGrain>,
    /// The input metric to accumulate
    pub metric: Option<MetricInput>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggregationType;

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
