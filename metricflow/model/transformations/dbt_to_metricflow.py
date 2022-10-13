from dataclasses import dataclass
from operator import xor
import traceback
from typing import Dict, List, Optional, Set
from dbt.contracts.graph.parsed import ParsedMetric as DbtMetric, ParsedModelNode as DbtModelNode
from dbt.contracts.graph.unparsed import MetricFilter as DbtMetricFilter
from dbt.exceptions import ref_invalid_args
from dbt.contracts.graph.manifest import Manifest as DbtManifest
from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.metric import Metric, MetricInputMeasure, MetricType, MetricTypeParams
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationError, ValidationIssue
from metricflow.time.time_granularity import TimeGranularity


@dataclass
class TransformedDbtMetric:  # noqa: D
    data_source: DataSource
    metric: Metric


CALC_METHOD_TO_MEASURE_TYPE: Dict[str, AggregationType] = {
    "count": AggregationType.COUNT,
    "count_distinct": AggregationType.COUNT_DISTINCT,
    "sum": AggregationType.SUM,
    "average": AggregationType.AVERAGE,
    "min": AggregationType.MIN,
    "max": AggregationType.MAX,
    # "derived": AggregationType.DERIVED # Derived DBT metrics don't create measures
}


class DbtManifestTransformer:
    """The DbtManifestTransform is a class used to transform dbt Manifests into MetricFlow UserConfiguredModels

    This helps keep track of state objects while transforming the Manifest into a
    UserConfiguredModel, ensuring like dbt Node elements are rendered only once and
    allowing us to pass around fewer arguments (reducing the mental load)
    """

    def __init__(self, manifest: DbtManifest) -> None:
        """Constructor.

        Args:
            manifest: A dbt Manifest object
        """
        self.manifest = manifest
        self._time_dimension_stats: Optional[Dict[str, List[str]]] = None  # lazy load it
        self._resolved_dbt_model_refs: Dict[int, DbtModelNode] = {}  # cache of resolved nodes

    @property
    def time_dimension_stats(self) -> Dict[str, List[str]]:
        """The stats on time dimensions from the dbt Manifest

        The time dimension stats are returned as a dictionary where in the keys
        are the names of the time dimension, and the value associated with each
        key is a list of `DbtMetric.model`s which that the associated time
        dimension should be primary for
        """
        if not self._time_dimension_stats:
            self._time_dimension_stats = self.collect_time_dimension_stats(dbt_metrics=self.manifest.metrics.values())

        return self._time_dimension_stats

    def resolve_metric_model_ref(self, dbt_metric: DbtMetric) -> DbtModelNode:
        """Returns a DbtModelNode based on the `DbtMetric.model` ref

        There should be a way to do this using dbt, and ideally we'll transition
        to doing this whatever that way is, but for the time being this was the
        fastest solution time wise. In addition to resolving the ref, this method
        caches the resolved ref node and uses the cache when possible to avoid re-
        resolving nodes.
        """
        if dbt_metric.model[:4] != "ref(":
            raise RuntimeError("Can only resolve refs for ref strings that begin with `ref(`")

        target_model = None
        target_package = None

        # Parse the DbtMetric.model into it's parts
        ref_parts = dbt_metric.model[4:-1].split(",")
        if len(ref_parts) == 1:
            target_model = ref_parts[0].strip(" \"'\t\r\n")
        elif len(ref_parts) == 2:
            target_package = ref_parts[0].strip(" \"'\t\r\n")
            target_model = ref_parts[1].strip(" \"'\t\r\n")
        else:
            ref_invalid_args(dbt_metric.name, ref_parts)

        hashed = hash((target_model, target_package, self.manifest.metadata.project_id, dbt_metric.package_name))
        if hashed not in self._resolved_dbt_model_refs:
            node = self.manifest.resolve_ref(
                target_model_name=target_model,
                target_model_package=target_package,
                current_project=self.manifest.metadata.project_id,
                node_package=dbt_metric.package_name,
            )
            assert isinstance(
                node, DbtModelNode
            ), f"Ref `{dbt_metric.model}` resolved to {node}, which is not of type `{DbtModelNode.__name__}`"

            self._resolved_dbt_model_refs[hashed] = node

        return self._resolved_dbt_model_refs[hashed]

    @classmethod
    def db_table_from_model_node(cls, node: DbtModelNode) -> str:
        """Get the '.' joined database table name of a DbtModelNode"""
        return f"{node.database}.{node.schema}.{node.name}"

    def _build_dimension(self, name: str, dbt_metric: DbtMetric) -> Dimension:  # noqa: D
        if name in self.time_dimension_stats.keys():
            return Dimension(
                name=name,
                type=DimensionType.TIME,
                type_params=DimensionTypeParams(
                    is_primary=dbt_metric.model in self.time_dimension_stats[name], time_granularity=TimeGranularity.DAY
                ),
            )
        else:
            return Dimension(
                name=name,
                type=DimensionType.CATEGORICAL,
            )

    def _build_dimensions(self, dbt_metric: DbtMetric) -> List[Dimension]:  # noqa: D
        dimensions = []

        # Build dimensions specifically from DbtMetric.dimensions list
        for dimension in dbt_metric.dimensions:
            dimensions.append(self._build_dimension(name=dimension, dbt_metric=dbt_metric))

        # Add DbtMetric.timestamp as a time dimension
        dimensions.append(self._build_dimension(name=dbt_metric.timestamp, dbt_metric=dbt_metric))

        # We need to deduplicate the filters because a field could be the same in
        # two filters. For example, if two filters exist for `amount`, one with
        # `>= 500` and the other `< 1000`, but only one dimension should be created
        distinct_dbt_metric_filter_fields = set([filter.field for filter in dbt_metric.filters])
        # Add dimension per distinct filter field
        # exclude when field is also listed as a DbtMetric.dimension
        # exclude when field is also the DbtMetric.timestamp
        for filter_field in distinct_dbt_metric_filter_fields:
            if filter_field not in dbt_metric.dimensions and filter_field != dbt_metric.timestamp:
                dimensions.append(self._build_dimension(name=filter_field, dbt_metric=dbt_metric))

        return dimensions

    def _build_measure(self, dbt_metric: DbtMetric) -> Measure:  # noqa: D
        return Measure(
            name=dbt_metric.name,
            agg=CALC_METHOD_TO_MEASURE_TYPE[dbt_metric.calculation_method],
            expr=dbt_metric.expression,
            agg_time_dimension=dbt_metric.timestamp,
        )

    def _build_data_source(self, dbt_metric: DbtMetric) -> DataSource:  # noqa: D
        metric_model_ref = self.resolve_metric_model_ref(dbt_metric=dbt_metric)
        data_source_table = self.db_table_from_model_node(metric_model_ref)
        return DataSource(
            name=metric_model_ref.name,
            description=metric_model_ref.description,
            sql_table=data_source_table,
            dbt_model=data_source_table,
            dimensions=self._build_dimensions(dbt_metric),
            measures=[self._build_measure(dbt_metric)],
        )

    def _where_clause_from_filters(self, filters: List[DbtMetricFilter]) -> str:  # noqa: D
        clauses = [f"{filter.field} {filter.operator} {filter.value}" for filter in filters]
        return " AND ".join(clauses)

    def _build_proxy_metric(self, dbt_metric: DbtMetric) -> Metric:  # noqa: D
        where_clause_constraint: Optional[WhereClauseConstraint] = None
        if dbt_metric.filters:
            where_clause_constraint = WhereClauseConstraint(
                where=self._where_clause_from_filters(filters=dbt_metric.filters),
                linkable_names=[filter.field for filter in dbt_metric.filters],
            )

        return Metric(
            name=dbt_metric.name,
            description=dbt_metric.description,
            type=MetricType.MEASURE_PROXY,
            type_params=MetricTypeParams(
                measure=MetricInputMeasure(name=dbt_metric.name),
            ),
            constraint=where_clause_constraint,
        )

    def dbt_metric_to_metricflow_elements(self, dbt_metric: DbtMetric) -> TransformedDbtMetric:  # noqa: D
        data_source = self._build_data_source(dbt_metric)
        proxy_metric = self._build_proxy_metric(dbt_metric)
        return TransformedDbtMetric(data_source=data_source, metric=proxy_metric)

    @classmethod
    def merge_data_sources(cls, data_sources: List[DataSource]) -> DataSource:  # noqa: D
        if len(data_sources) == 1:
            return data_sources[0]

        measures: Set[Measure] = set()
        identifiers: Set[Identifier] = set()
        dimensions: Set[Dimension] = set()
        names: List[str] = []
        descriptions: List[str] = []
        sql_tables: List[str] = []
        sql_queries: List[str] = []
        dbt_models: List[str] = []
        for data_source in data_sources:
            names.append(data_source.name) if data_source.name else None
            descriptions.append(data_source.description) if data_source.description else None
            sql_tables.append(data_source.sql_table) if data_source.sql_table else None
            sql_queries.append(data_source.sql_query) if data_source.sql_query else None
            dbt_models.append(data_source.dbt_model) if data_source.dbt_model else None
            measures = measures.union(set(data_source.measures)) if data_source.measures else measures
            identifiers = identifiers.union(set(data_source.identifiers)) if data_source.identifiers else identifiers
            dimensions = dimensions.union(set(data_source.dimensions)) if data_source.dimensions else dimensions

        set_names = set(names)
        set_descriptions = set(descriptions)
        set_sql_tables = set(sql_tables)
        set_sql_queries = set(sql_queries)
        set_dbt_models = set(dbt_models)

        assert len(set_names) == 1, "Cannot merge data sources, all data sources to merge must have same name"
        assert (
            len(set_descriptions) <= 1
        ), "Cannot merge data sources, all data sources to merge must have same descritpion (or none)"
        assert (
            len(set_sql_tables) <= 1
        ), "Cannot merge data sources, all data sources to merge must have same sql_table (or none)"
        assert (
            len(set_sql_queries) <= 1
        ), "Cannot merge data sources, all data sources to merge must have same sql_query (or none)"
        assert xor(
            len(set_sql_tables) == 1, len(set_sql_queries) == 1
        ), "Cannot merge data sources, definitions for both sql_table and sql_query exist"
        assert (
            len(set_dbt_models) <= 1
        ), "Cannot merge data sources, all data sources to merge must have same dbt_model (or none)"

        return DataSource(
            name=list(set_names)[0],
            description=list(set_descriptions)[0] if set_descriptions else None,
            sql_table=list(set_sql_tables)[0] if set_sql_tables else None,
            sql_query=list(set_sql_queries)[0] if set_sql_queries else None,
            dbt_model=list(set_dbt_models)[0] if set_dbt_models else None,
            dimensions=list(dimensions),
            identifiers=list(identifiers),
            measures=list(measures),
        )

    @classmethod
    def collect_time_dimension_stats(cls, dbt_metrics: List[DbtMetric]) -> Dict[str, List[str]]:
        """Compiles stats on time dimensions for a given list of DbtMetrics

        Arguably this is probably a fairly black magic function. This function
        determines which time dimensions are primary for which `DbtMetric.model`
        """
        # For storing the time dimension names and the `DbtMetric.model`s they are primary for
        time_dimensions: Dict[str, List[str]] = {}

        # Map each `DbtMetric.model` value to counts of how many times each time
        # dimension is associated with it. The time dimension that is associated
        # with a `DbtMetric.model` the most will be considered the primary time
        # dimenson for the data source that is built for the `DbtMetric.model`
        time_stats_for_metric_models: Dict[str, Dict[str, int]] = {}
        for dbt_metric in dbt_metrics:
            if dbt_metric.calculation_method != "derived":
                if dbt_metric.timestamp not in time_dimensions:
                    time_dimensions[dbt_metric.timestamp] = []

                if dbt_metric.model not in time_stats_for_metric_models:
                    time_stats_for_metric_models[dbt_metric.model] = {dbt_metric.timestamp: 1}
                else:
                    time_stats_for_metric_models[dbt_metric.model][dbt_metric.timestamp] += 1

        # Take the mapping created above and set the `DbtMetric.model` values
        # that should be primary for the time dimension
        for metric_model, time_stats in time_stats_for_metric_models.items():
            primary_time_dim = max(time_stats, key=time_stats.get)  # type: ignore
            time_dimensions[primary_time_dim].append(metric_model)

        return time_dimensions

    def build_user_configured_model(self) -> ModelBuildResult:  # noqa: D
        data_sources_map: Dict[str, List[DataSource]] = {}
        metrics = []
        issues: List[ValidationIssue] = []

        for dbt_metric in self.manifest.metrics.values():
            # TODO: Handle derived dbt metrics
            if dbt_metric.calculation_method == "derived":
                continue
            else:
                transformed_dbt_metric = self.dbt_metric_to_metricflow_elements(dbt_metric=dbt_metric)
                if transformed_dbt_metric.data_source.name not in data_sources_map:
                    data_sources_map[transformed_dbt_metric.data_source.name] = [transformed_dbt_metric.data_source]
                else:
                    data_sources_map[transformed_dbt_metric.data_source.name].append(transformed_dbt_metric.data_source)
                metrics.append(transformed_dbt_metric.metric)

        # As it might be the case that we generated many of the same data source,
        # we need to merge / dedupe them
        deduped_data_sources = []
        for name, data_sources in data_sources_map.items():
            try:
                deduped_data_sources.append(self.merge_data_sources(data_sources))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to merge data sources with the name `{name}`",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        return ModelBuildResult(
            model=UserConfiguredModel(data_sources=list(deduped_data_sources), metrics=metrics),
            issues=ModelValidationResults.from_issues_sequence(issues=issues),
        )
