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
