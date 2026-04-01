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
