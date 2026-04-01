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

        assert_eq!(manifest.semantic_models.len(), 12);
        assert!(manifest.metrics.len() > 50, "expected many metrics from the full manifest");

        let model = manifest
            .semantic_models
            .iter()
            .find(|m| m.name == "bookings_source")
            .expect("bookings_source model should exist");
        assert_eq!(model.node_relation.fully_qualified(), "demo.fct_bookings");
        assert_eq!(model.primary_entity.as_deref(), Some("booking"));

        // bookings_source has multiple measures in the full manifest
        let bookings_measure = model
            .measures
            .iter()
            .find(|m| m.name == "bookings")
            .expect("bookings measure should exist");
        assert_eq!(bookings_measure.agg, AggregationType::Sum);
        assert_eq!(bookings_measure.sql_expr(), "1");

        let ds_dim = model
            .dimensions
            .iter()
            .find(|d| d.name == "ds")
            .expect("ds dimension should exist");
        assert_eq!(ds_dim.dimension_type, DimensionType::Time);

        let is_instant_dim = model
            .dimensions
            .iter()
            .find(|d| d.name == "is_instant")
            .expect("is_instant dimension should exist");
        assert_eq!(is_instant_dim.dimension_type, DimensionType::Categorical);

        let listing_entity = model
            .entities
            .iter()
            .find(|e| e.name == "listing")
            .expect("listing entity should exist");
        assert_eq!(listing_entity.entity_type, EntityType::Foreign);

        let metric = manifest
            .metrics
            .iter()
            .find(|m| m.name == "bookings")
            .expect("bookings metric should exist");
        assert_eq!(metric.metric_type, MetricKind::Simple);
        assert_eq!(
            metric.type_params.measure.as_ref().unwrap().name,
            "bookings"
        );
    }
}
