use mf_core::manifest::*;
use mf_core::types::{EntityType, TimeGrain};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("duplicate metric name: {0}")]
    DuplicateMetric(String),
    #[error("metric '{0}' references unknown measure '{1}'")]
    UnknownMeasure(String, String),
}

/// Describes a single entity join: left model has a FOREIGN entity that pairs
/// with a PRIMARY entity of the same name on the right model.
#[derive(Debug, Clone)]
pub struct EntityJoin<'a> {
    /// The entity name shared by both sides (e.g. "listing")
    pub entity_name: &'a str,
    /// The model that holds the FOREIGN entity (join left side)
    pub left_model: &'a SemanticModel,
    /// SQL expression from the left side (e.g. "listing_id")
    pub left_expr: &'a str,
    /// The model that holds the PRIMARY entity (join right side)
    pub right_model: &'a SemanticModel,
    /// SQL expression from the right side (e.g. "listing_id")
    pub right_expr: &'a str,
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
    /// Maps primary/unique entity name → model ref
    models_by_entity: HashMap<&'a str, &'a SemanticModel>,
    /// Maps (left_model_name, entity_name) → list of EntityJoins.
    /// Multiple right-side models may match the same foreign entity name.
    entity_joins: HashMap<(&'a str, &'a str), Vec<EntityJoin<'a>>>,
}

impl<'a> SemanticGraph<'a> {
    pub fn build(manifest: &'a SemanticManifest) -> Result<Self, GraphError> {
        let mut metrics_by_name = HashMap::new();
        for metric in &manifest.metrics {
            if metrics_by_name
                .insert(metric.name.as_str(), metric)
                .is_some()
            {
                return Err(GraphError::DuplicateMetric(metric.name.clone()));
            }
        }

        let mut models_by_measure: HashMap<&str, Vec<&SemanticModel>> = HashMap::new();
        let mut dimensions_by_model: HashMap<(&str, &str), &Dimension> = HashMap::new();
        let mut models_by_name = HashMap::new();
        let mut models_by_entity: HashMap<&str, &SemanticModel> = HashMap::new();

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
            // Index primary/unique entities for filter resolution
            for entity in &model.entities {
                if matches!(
                    entity.entity_type,
                    EntityType::Primary | EntityType::Unique
                ) {
                    models_by_entity.insert(entity.name.as_str(), model);
                }
            }
            // Also index via top-level primary_entity field (may not appear in entities list)
            if let Some(ref pe) = model.primary_entity {
                models_by_entity.entry(pe.as_str()).or_insert(model);
            }
        }

        // Build entity join index: pair FOREIGN entities on one model with PRIMARY
        // entities of the same name on another model.
        let mut entity_joins: HashMap<(&str, &str), Vec<EntityJoin>> = HashMap::new();

        // Collect (entity_name, EntityType, expr, model) for all entities
        let mut primary_entities: Vec<(&str, &str, &SemanticModel)> = Vec::new(); // (entity_name, expr, model)
        let mut foreign_entities: Vec<(&str, &str, &SemanticModel)> = Vec::new();

        for model in &manifest.semantic_models {
            for entity in &model.entities {
                let expr = entity.sql_expr();
                match entity.entity_type {
                    EntityType::Primary | EntityType::Unique => {
                        primary_entities.push((entity.name.as_str(), expr, model));
                    }
                    EntityType::Foreign | EntityType::Natural => {
                        foreign_entities.push((entity.name.as_str(), expr, model));
                    }
                }
            }
        }

        // For each foreign entity, find ALL matching primary entities with the same name
        for (foreign_name, foreign_expr, foreign_model) in &foreign_entities {
            for (primary_name, primary_expr, primary_model) in &primary_entities {
                if *foreign_name == *primary_name && foreign_model.name != primary_model.name {
                    entity_joins
                        .entry((foreign_model.name.as_str(), foreign_name))
                        .or_default()
                        .push(EntityJoin {
                            entity_name: foreign_name,
                            left_model: foreign_model,
                            left_expr: foreign_expr,
                            right_model: primary_model,
                            right_expr: primary_expr,
                        });
                }
            }
        }

        Ok(Self {
            manifest,
            metrics_by_name,
            models_by_measure,
            dimensions_by_model,
            models_by_name,
            models_by_entity,
            entity_joins,
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

    /// Find the model that has a primary/unique entity with the given name.
    pub fn find_model_by_entity(&self, entity_name: &str) -> Option<&'a SemanticModel> {
        self.models_by_entity.get(entity_name).copied()
    }

    /// Find a dimension on a specific model. Returns (model, dimension) if found.
    pub fn find_dimension(
        &self,
        dim_name: &str,
        model_name: &str,
    ) -> Option<(&'a SemanticModel, &'a Dimension)> {
        let dim = self
            .dimensions_by_model
            .get(&(model_name, dim_name))
            .copied()?;
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

    /// Find all entity joins where `left_model_name` is the foreign side.
    pub fn find_entity_joins(&self, left_model_name: &str) -> Vec<&EntityJoin<'a>> {
        self.entity_joins
            .iter()
            .filter(|((model_name, _), _)| *model_name == left_model_name)
            .flat_map(|(_, joins)| joins.iter())
            .collect()
    }

    /// Find the join path from `left_model_name` to a model that has `dim_name`.
    /// Returns `Some(EntityJoin)` if a one-hop join exists; `None` if already on the
    /// left model or if no path is found.
    pub fn find_join_path(&self, left_model_name: &str, dim_name: &str) -> Option<&EntityJoin<'a>> {
        // If the dimension is local, no join needed
        if self
            .dimensions_by_model
            .contains_key(&(left_model_name, dim_name))
        {
            return None;
        }

        // Search all joins from left_model_name; find one whose right side has dim_name
        self.find_entity_joins(left_model_name)
            .into_iter()
            .find(|join| {
                self.dimensions_by_model
                    .contains_key(&(join.right_model.name.as_str(), dim_name))
            })
    }

    pub fn manifest(&self) -> &'a SemanticManifest {
        self.manifest
    }

    /// Find the time spine table configuration for a given grain.
    /// Checks `time_spines` (newer format) first, then falls back to
    /// `time_spine_table_configurations` (older format).
    pub fn find_time_spine(&self, grain: TimeGrain) -> Option<TimeSpineInfo> {
        // Check time_spines first (newer format)
        for ts in &self.manifest.project_configuration.time_spines {
            if ts.primary_column.time_granularity <= grain {
                return Some(TimeSpineInfo {
                    table: ts.node_relation.fully_qualified(),
                    column: ts.primary_column.name.clone(),
                    grain: ts.primary_column.time_granularity,
                });
            }
        }
        // Fall back to time_spine_table_configurations (older format)
        for cfg in &self
            .manifest
            .project_configuration
            .time_spine_table_configurations
        {
            if cfg.grain <= grain {
                return Some(TimeSpineInfo {
                    table: cfg.location.clone(),
                    column: cfg.column_name.clone(),
                    grain: cfg.grain,
                });
            }
        }
        None
    }
}

/// Information about a time spine usable for cumulative metric computation.
#[derive(Debug, Clone)]
pub struct TimeSpineInfo {
    pub table: String,
    pub column: String,
    pub grain: TimeGrain,
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

    #[test]
    fn test_entity_join_index_two_models() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // bookings_source has listing as FOREIGN; listings_source has listing as PRIMARY
        let joins = graph.find_entity_joins("bookings_source");
        assert_eq!(joins.len(), 1);
        let join = joins[0];
        assert_eq!(join.entity_name, "listing");
        assert_eq!(join.left_model.name, "bookings_source");
        assert_eq!(join.left_expr, "listing_id");
        assert_eq!(join.right_model.name, "listings_source");
        assert_eq!(join.right_expr, "listing_id");
    }

    #[test]
    fn test_entity_join_index_for_bookings_source() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // The full simple_manifest has multiple models sharing entities,
        // so bookings_source should have joins available.
        let joins = graph.find_entity_joins("bookings_source");
        assert!(
            !joins.is_empty(),
            "bookings_source should have entity joins in the full manifest"
        );

        // Specifically check: bookings_source has 'listing' joins
        let listing_joins: Vec<_> = joins.iter().filter(|j| j.entity_name == "listing").collect();
        assert!(
            !listing_joins.is_empty(),
            "bookings_source should have 'listing' joins. Available: {:?}",
            joins.iter().map(|j| (j.entity_name, &j.right_model.name)).collect::<Vec<_>>()
        );

        // find_join_path should find country_latest via the listings_latest join
        let path = graph.find_join_path("bookings_source", "country_latest");
        assert!(
            path.is_some(),
            "should find join path from bookings_source to country_latest. Listing joins: {:?}",
            listing_joins.iter().map(|j| (&j.right_model.name, j.right_model.dimensions.iter().map(|d| &d.name).collect::<Vec<_>>())).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_find_join_path_local_dimension_returns_none() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // is_instant is on bookings_source — no join needed
        let path = graph.find_join_path("bookings_source", "is_instant");
        assert!(path.is_none());
    }

    #[test]
    fn test_find_join_path_cross_model_dimension() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // country is on listings_source — join via listing entity
        let path = graph.find_join_path("bookings_source", "country");
        assert!(path.is_some());
        let join = path.unwrap();
        assert_eq!(join.entity_name, "listing");
        assert_eq!(join.right_model.name, "listings_source");
    }

    #[test]
    fn test_find_join_path_unknown_dimension_returns_none() {
        let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        let path = graph.find_join_path("bookings_source", "nonexistent_dim");
        assert!(path.is_none());
    }

    #[test]
    fn test_find_time_spine_day_grain() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // The cumulative manifest has a day-grain time spine at demo.mf_time_spine
        let info = graph.find_time_spine(TimeGrain::Day);
        assert!(info.is_some(), "should find a day-grain time spine");
        let info = info.unwrap();
        assert_eq!(info.table, "demo.mf_time_spine");
        assert_eq!(info.column, "ds");
        assert_eq!(info.grain, TimeGrain::Day);
    }

    #[test]
    fn test_find_time_spine_month_uses_day_spine() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // Day spine (grain <= Month) can serve a month-grain query
        let info = graph.find_time_spine(TimeGrain::Month);
        assert!(info.is_some(), "day spine should serve month grain queries");
        let info = info.unwrap();
        assert_eq!(info.table, "demo.mf_time_spine");
    }

    #[test]
    fn test_find_time_spine_day_grain_simple_manifest() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = SemanticGraph::build(&manifest).unwrap();

        // The full simple_manifest has time spines configured (including day grain).
        let info = graph.find_time_spine(TimeGrain::Day);
        assert!(
            info.is_some(),
            "full simple_manifest should have a day-grain time spine"
        );
    }
}
