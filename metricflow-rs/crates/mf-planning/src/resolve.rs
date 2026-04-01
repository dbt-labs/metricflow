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
    #[error("metric '{0}' is not a derived metric")]
    NotDerivedMetric(String),
    #[error("metric '{0}' is not a ratio metric")]
    NotRatioMetric(String),
    #[error("metric '{0}' is not a cumulative metric")]
    NotCumulativeMetric(String),
    #[error("derived metric '{0}' has no expression")]
    NoExpression(String),
    #[error("derived metric '{0}' has no input metrics")]
    NoInputMetrics(String),
    #[error("ratio metric '{0}' has no numerator")]
    NoNumerator(String),
    #[error("ratio metric '{0}' has no denominator")]
    NoDenominator(String),
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

/// Resolved information needed to build a dataflow plan for a derived metric.
#[derive(Debug)]
pub struct ResolvedDerivedMetric<'a> {
    pub metric: &'a Metric,
    pub expr: String,
    /// (metric_name, alias) pairs for each input metric
    pub inputs: Vec<(String, String)>,
}

/// Resolve a derived metric: validate type, extract expression and input metrics.
pub fn resolve_derived_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedDerivedMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Derived {
        return Err(ResolveError::NotDerivedMetric(metric_name.into()));
    }

    let expr = metric
        .type_params
        .expr
        .clone()
        .ok_or_else(|| ResolveError::NoExpression(metric_name.into()))?;

    let metric_inputs = metric
        .type_params
        .metrics
        .as_ref()
        .ok_or_else(|| ResolveError::NoInputMetrics(metric_name.into()))?;

    if metric_inputs.is_empty() {
        return Err(ResolveError::NoInputMetrics(metric_name.into()));
    }

    let inputs = metric_inputs
        .iter()
        .map(|input| {
            let alias = input.alias.clone().unwrap_or_else(|| input.name.clone());
            (input.name.clone(), alias)
        })
        .collect();

    Ok(ResolvedDerivedMetric {
        metric,
        expr,
        inputs,
    })
}

/// Resolved information needed to build a dataflow plan for a ratio metric.
#[derive(Debug)]
pub struct ResolvedRatioMetric<'a> {
    pub metric: &'a Metric,
    /// (metric_name, alias) for numerator
    pub numerator: (String, String),
    /// (metric_name, alias) for denominator
    pub denominator: (String, String),
}

/// Resolve a ratio metric: validate type, extract numerator and denominator.
pub fn resolve_ratio_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedRatioMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Ratio {
        return Err(ResolveError::NotRatioMetric(metric_name.into()));
    }

    let numerator_input = metric
        .type_params
        .numerator
        .as_ref()
        .ok_or_else(|| ResolveError::NoNumerator(metric_name.into()))?;

    let denominator_input = metric
        .type_params
        .denominator
        .as_ref()
        .ok_or_else(|| ResolveError::NoDenominator(metric_name.into()))?;

    let numerator = (
        numerator_input.name.clone(),
        numerator_input
            .alias
            .clone()
            .unwrap_or_else(|| numerator_input.name.clone()),
    );

    let denominator = (
        denominator_input.name.clone(),
        denominator_input
            .alias
            .clone()
            .unwrap_or_else(|| denominator_input.name.clone()),
    );

    Ok(ResolvedRatioMetric {
        metric,
        numerator,
        denominator,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mf_manifest::parse;

    #[test]
    fn test_resolve_simple_metric() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_simple_metric(&graph, "bookings").unwrap();
        assert_eq!(resolved.metric.name, "bookings");
        assert_eq!(resolved.measure.name, "bookings");
        assert_eq!(resolved.model.name, "bookings_source");
    }

    #[test]
    fn test_resolve_simple_metric_unknown() {
        let json = include_str!("../../../tests/fixtures/simple_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let result = resolve_simple_metric(&graph, "nonexistent");
        assert!(matches!(
            result.unwrap_err(),
            ResolveError::UnknownMetric(_)
        ));
    }

    #[test]
    fn test_resolve_derived_metric() {
        let json = include_str!("../../../tests/fixtures/derived_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_derived_metric(&graph, "bookings_growth").unwrap();
        assert_eq!(resolved.metric.name, "bookings_growth");
        assert_eq!(resolved.expr, "bookings - instant_bookings");
        assert_eq!(resolved.inputs.len(), 2);
        assert_eq!(
            resolved.inputs[0],
            ("bookings".to_string(), "bookings".to_string())
        );
        assert_eq!(
            resolved.inputs[1],
            (
                "instant_bookings".to_string(),
                "instant_bookings".to_string()
            )
        );
    }

    #[test]
    fn test_resolve_derived_metric_not_derived_error() {
        let json = include_str!("../../../tests/fixtures/derived_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        // "bookings" is a simple metric, not derived
        let result = resolve_derived_metric(&graph, "bookings");
        assert!(matches!(
            result.unwrap_err(),
            ResolveError::NotDerivedMetric(_)
        ));
    }

    #[test]
    fn test_resolve_ratio_metric() {
        let json = include_str!("../../../tests/fixtures/ratio_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_ratio_metric(&graph, "instant_booking_rate").unwrap();
        assert_eq!(resolved.metric.name, "instant_booking_rate");
        assert_eq!(
            resolved.numerator,
            (
                "instant_bookings".to_string(),
                "instant_bookings".to_string()
            )
        );
        assert_eq!(
            resolved.denominator,
            ("bookings".to_string(), "bookings".to_string())
        );
    }

    #[test]
    fn test_resolve_ratio_metric_not_ratio_error() {
        let json = include_str!("../../../tests/fixtures/ratio_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        // "bookings" is a simple metric, not a ratio
        let result = resolve_ratio_metric(&graph, "bookings");
        assert!(matches!(
            result.unwrap_err(),
            ResolveError::NotRatioMetric(_)
        ));
    }
}
