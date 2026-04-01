use mf_core::manifest::*;
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
