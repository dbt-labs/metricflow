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
    #[error("model '{0}' not found")]
    ModelNotFound(String),
}

/// Owned measure information extracted from either the model's measures list
/// (older manifest format) or from `metric_aggregation_params` (newer format).
#[derive(Debug, Clone)]
pub struct ResolvedMeasureInfo {
    pub name: String,
    pub agg: AggregationType,
    pub expr: String,
    pub fill_nulls_with: Option<i64>,
}

/// Resolved information needed to build a dataflow plan for a simple metric.
#[derive(Debug)]
pub struct ResolvedSimpleMetric<'a> {
    pub metric: &'a Metric,
    pub measure: ResolvedMeasureInfo,
    pub model: &'a SemanticModel,
    pub agg_time_dimension: Option<&'a Dimension>,
}

/// Resolve a simple metric: find its measure, source model, and time dimension.
/// Supports two manifest formats:
/// - Older: `type_params.measure` references a named measure on a semantic model
/// - Newer: `type_params.metric_aggregation_params` inlines measure info with a model reference
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

    // Path 1: older format — `measure` field references a named measure
    if let Some(measure_ref) = &metric.type_params.measure {
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

        let fill_nulls_with = measure_ref
            .fill_nulls_with
            .or(metric.type_params.fill_nulls_with);

        return Ok(ResolvedSimpleMetric {
            metric,
            measure: ResolvedMeasureInfo {
                name: measure.name.clone(),
                agg: measure.agg,
                expr: measure.sql_expr().to_string(),
                fill_nulls_with,
            },
            model,
            agg_time_dimension,
        });
    }

    // Path 2: newer format — `metric_aggregation_params` inlines measure info
    if let Some(params) = &metric.type_params.metric_aggregation_params {
        let model = graph
            .find_model(&params.semantic_model)
            .ok_or_else(|| ResolveError::ModelNotFound(params.semantic_model.clone()))?;

        let expr = params
            .expr
            .as_deref()
            .unwrap_or(&metric.name)
            .to_string();

        // Find agg_time_dimension from params or model defaults
        let agg_time_dimension = params
            .agg_time_dimension
            .as_deref()
            .or_else(|| {
                model
                    .defaults
                    .as_ref()
                    .and_then(|d| d.agg_time_dimension.as_deref())
            })
            .and_then(|name| model.dimensions.iter().find(|d| d.name == name));

        return Ok(ResolvedSimpleMetric {
            metric,
            measure: ResolvedMeasureInfo {
                name: metric.name.clone(),
                agg: params.agg,
                expr,
                fill_nulls_with: metric.type_params.fill_nulls_with,
            },
            model,
            agg_time_dimension,
        });
    }

    Err(ResolveError::NoMeasure(metric_name.into()))
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

/// Resolved information needed to build a dataflow plan for a cumulative metric.
#[derive(Debug)]
pub struct ResolvedCumulativeMetric<'a> {
    pub metric: &'a Metric,
    pub measure: ResolvedMeasureInfo,
    pub model: &'a SemanticModel,
    pub agg_time_dimension: Option<&'a Dimension>,
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<TimeGrain>,
}

/// Resolve a cumulative metric: validate type, find measure + model + time dimension,
/// and extract window/grain_to_date parameters.
/// Supports two manifest formats:
/// - Older: `type_params.measure` + top-level `window`/`grain_to_date`
/// - Newer: `type_params.cumulative_type_params` with nested `metric` reference
pub fn resolve_cumulative_metric<'a>(
    graph: &'a SemanticGraph<'a>,
    metric_name: &str,
) -> Result<ResolvedCumulativeMetric<'a>, ResolveError> {
    let metric = graph
        .find_metric(metric_name)
        .ok_or_else(|| ResolveError::UnknownMetric(metric_name.into()))?;

    if metric.metric_type != MetricKind::Cumulative {
        return Err(ResolveError::NotCumulativeMetric(metric_name.into()));
    }

    // Path 1: older format — direct `measure` field + top-level window/grain_to_date
    if let Some(measure_ref) = &metric.type_params.measure {
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

        let window = metric.type_params.window.clone();
        let grain_to_date = metric.type_params.grain_to_date;

        return Ok(ResolvedCumulativeMetric {
            metric,
            measure: ResolvedMeasureInfo {
                name: measure.name.clone(),
                agg: measure.agg,
                expr: measure.sql_expr().to_string(),
                fill_nulls_with: measure_ref
                    .fill_nulls_with
                    .or(metric.type_params.fill_nulls_with),
            },
            model,
            agg_time_dimension,
            window,
            grain_to_date,
        });
    }

    // Path 2: newer format — `cumulative_type_params` with nested metric reference
    if let Some(cum_params) = &metric.type_params.cumulative_type_params {
        let input_metric_ref = cum_params
            .metric
            .as_ref()
            .ok_or_else(|| ResolveError::NoMeasure(metric_name.into()))?;

        // Resolve the referenced input metric as a simple metric
        let input_resolved = resolve_simple_metric(graph, &input_metric_ref.name)?;

        let window = cum_params.window.clone();
        let grain_to_date = cum_params.grain_to_date;

        return Ok(ResolvedCumulativeMetric {
            metric,
            measure: input_resolved.measure,
            model: input_resolved.model,
            agg_time_dimension: input_resolved.agg_time_dimension,
            window,
            grain_to_date,
        });
    }

    Err(ResolveError::NoMeasure(metric_name.into()))
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

    #[test]
    fn test_resolve_cumulative_metric_with_window() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_cumulative_metric(&graph, "trailing_7d_bookings").unwrap();
        assert_eq!(resolved.metric.name, "trailing_7d_bookings");
        assert_eq!(resolved.measure.name, "bookings");
        assert_eq!(resolved.model.name, "bookings_source");
        assert!(resolved.window.is_some());
        let window = resolved.window.unwrap();
        assert_eq!(window.count, 7);
        assert_eq!(window.granularity, "day");
        assert!(resolved.grain_to_date.is_none());
    }

    #[test]
    fn test_resolve_cumulative_metric_grain_to_date() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_cumulative_metric(&graph, "bookings_mtd").unwrap();
        assert_eq!(resolved.metric.name, "bookings_mtd");
        assert!(resolved.window.is_none());
        assert_eq!(resolved.grain_to_date, Some(TimeGrain::Month));
    }

    #[test]
    fn test_resolve_cumulative_metric_all_time() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_cumulative_metric(&graph, "bookings_all_time").unwrap();
        assert_eq!(resolved.metric.name, "bookings_all_time");
        assert!(resolved.window.is_none());
        assert!(resolved.grain_to_date.is_none());
    }

    #[test]
    fn test_resolve_cumulative_metric_not_cumulative_error() {
        let json = include_str!("../../../tests/fixtures/cumulative_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        // bookings is a simple metric, not cumulative
        let result = resolve_cumulative_metric(&graph, "bookings");
        // The metric doesn't exist in the cumulative manifest, so we get UnknownMetric
        assert!(result.is_err());
    }

    // --- Tests for real manifest format (metric_aggregation_params / cumulative_type_params) ---

    #[test]
    fn test_resolve_simple_metric_real_format() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_simple_metric(&graph, "arr_current").unwrap();
        assert_eq!(resolved.metric.name, "arr_current");
        assert_eq!(resolved.measure.agg, AggregationType::Sum);
        assert_eq!(resolved.measure.expr, "month_ending_current_arr");
        assert_eq!(resolved.model.name, "fct_customer_arr_waterfall");
        // agg_time_dimension should resolve to the "close_month" dimension
        assert!(resolved.agg_time_dimension.is_some());
        assert_eq!(resolved.agg_time_dimension.unwrap().name, "close_month");
        // The dimension's SQL expr is "date_month"
        assert_eq!(
            resolved.agg_time_dimension.unwrap().sql_expr(),
            "date_month"
        );
    }

    #[test]
    fn test_resolve_simple_metric_real_format_no_measure_field() {
        // Verify that when measure is null and metric_aggregation_params is present, it works
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        // arr_current has "measure": null — must resolve via metric_aggregation_params
        let metric = graph.find_metric("arr_current").unwrap();
        assert!(metric.type_params.measure.is_none());
        assert!(metric.type_params.metric_aggregation_params.is_some());

        let resolved = resolve_simple_metric(&graph, "arr_current").unwrap();
        assert_eq!(resolved.model.name, "fct_customer_arr_waterfall");
    }

    #[test]
    fn test_resolve_cumulative_metric_real_format() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_cumulative_metric(&graph, "trailing_30d_arr").unwrap();
        assert_eq!(resolved.metric.name, "trailing_30d_arr");
        // Should resolve through the input metric (arr_current) to the underlying measure
        assert_eq!(resolved.measure.agg, AggregationType::Sum);
        assert_eq!(resolved.measure.expr, "month_ending_current_arr");
        assert_eq!(resolved.model.name, "fct_customer_arr_waterfall");
        // Window from cumulative_type_params
        assert!(resolved.window.is_some());
        let window = resolved.window.unwrap();
        assert_eq!(window.count, 30);
        assert_eq!(window.granularity, "day");
        assert!(resolved.grain_to_date.is_none());
    }

    #[test]
    fn test_resolve_derived_metric_with_real_format_inputs() {
        let json = include_str!("../../../tests/fixtures/real_format_manifest.json");
        let manifest = parse::from_json(json).unwrap();
        let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

        let resolved = resolve_derived_metric(&graph, "arr_growth").unwrap();
        assert_eq!(resolved.metric.name, "arr_growth");
        assert_eq!(resolved.expr, "arr_current - arr_new");
        assert_eq!(resolved.inputs.len(), 2);
        // Input metrics use metric_aggregation_params format
        // but derived resolution just extracts names, doesn't resolve inputs
        assert_eq!(
            resolved.inputs[0],
            ("arr_current".to_string(), "arr_current".to_string())
        );
        assert_eq!(
            resolved.inputs[1],
            ("arr_new".to_string(), "arr_new".to_string())
        );
    }
}
