<p align="center">
<img src="https://github.com/transform-data/metricflow/raw/main/assets/MetricFlow_logo.png" />
<br /><br />
</p>

# Welcome to MetricFlow

See our latest updates in the [Metricflow Changelog](https://github.com/transform-data/metricflow/blob/main/CHANGELOG.md)!

MetricFlow translates a simple metric definition into reusable SQL, and executes it against the SQL engine of your choice. This makes it easy to get consistent metric output broken down by attributes (dimensions) of interest.

MetricFlow is a computational framework for building and maintaining consistent metric logic. The name comes from the approach taken to generate metrics. Using the user-defined semantic model, a query is first compiled into a metric dataflow plan. The plan is then converted to an abstract SQL object model, optimized, and rendered to engine-specific SQL.

MetricFlow provides a set of abstractions that help you construct complicated logic and dynamically generate queries to handle:

- Complex metric types such as ratio, expression, and cumulative
- Multi-hop joins between fact and dimension sources
- Metric aggregation to different time granularities
- And so much more

As a developer, you can also use MetricFlow's interfaces to construct APIs for integrations to bring metrics into downstream tools in your data stack.

MetricFlow itself acts as a semantic layer, compiling the semantic information described in the MetricFlow spec to SQL that can be executed against the data warehouse and served to downstream applications. It acts as a proxy, translating metric requests in the form of “metrics by dimensions” into SQL queries that traverse the data warehouse and the underlying semantic structure to resolve every possible combination of metric and dimension.

## Getting Started

### Install MetricFlow

If you do not have postgres on your machine, first install Postgres:
- Postgres provides [pre-built packages for download and installation](https://www.postgresql.org/download/)
- Mac users might prefer to use Homebrew: `brew install postgresql`

If you would like to visualize metric dataflow plans via CLI, install Graphviz:
- Graphviz provides [pre-built packages for download and installation](https://www.graphviz.org/download/)
- Mac users might prefer to use Homebrew: `brew install graphviz`

The visualizations are in an early state of development, but look similar to:

<p align="center">
<img src="https://github.com/transform-data/metricflow/raw/main/assets/example_plan.svg" height="500"/>
<br /><br />
</p>

Then, proceed with the regular installation as follows:

MetricFlow can be installed from PyPi for use as a Python library with the following command:

```
pip install metricflow
```

Once installed, MetricFlow can be setup and connected to a data warehouse by following the instructions after issuing the command:

```
mf setup
```

In case you don't have a connection to a data warehouse available and want a self-contained demo, DuckDB can be selected.

To see what MetricFlow can do without custom configurations, start the tutorial by running:

```
mf tutorial
```

To get up and running with your own metrics, you should rely on MetricFlow’s documentation available at [MetricFlow docs](https://docs.transform.co/docs/metricflow/guides/introduction).

### Tutorial

```
mf tutorial # optionally add `--skip-dw` if you have already confirmed your datawarehouse connection works
```

For reference, the tutorial steps are below:

```
🤓 Please run the following steps,

    1.  In '{$HOME}/.metricflow/config.yml', `model_path` should be '{$HOME}/.metricflow/sample_models'.
    2.  Try validating your data model: `mf validate-configs`
    3.  Check out your metrics: `mf list-metrics`
    4.  Check out dimensions for your metric `mf list-dimensions --metric-names transactions`
    5.  Query your first metric: `mf query --metrics transactions --dimensions metric_time --order metric_time`
    6.  Show the SQL MetricFlow generates:
        `mf query --metrics transactions --dimensions metric_time --order metric_time --explain`
    7.  Visualize the plan:
        `mf query --metrics transactions --dimensions metric_time --order metric_time --explain --display-plans`
        * This only works if you have graphviz installed - see README.
    8.  Add another dimension:
        `mf query --metrics transactions --dimensions metric_time,customer__country --order metric_time`
    9.  Add a coarser time granularity:
        `mf query --metrics transactions --dimensions metric_time__week --order metric_time__week`
    10. Try a more complicated query:
        `mf query \
          --metrics transactions,transaction_usd_na,transaction_usd_na_l7d --dimensions metric_time,is_large \
          --order metric_time --start-time 2022-03-20 --end-time 2022-04-01`
        * You can also add `--explain --display-plans`.
    11. For more ways to interact with the sample models, go to
        ‘https://docs.transform.co/docs/metricflow/metricflow-tutorial’.
    12. Once you’re done, run `mf tutorial --skip-dw --drop-tables` to drop the sample tables.
```


## Core Tenets

The framework relies on a set of core tenets:

- **DRY (Don’t Repeat Yourself)**: This principle is the core objective of the underlying MetricFlow spec. Duplication of logic leads to incorrectly constructed metrics and should be avoided through thoughtfully-designed abstractions.
- **SQL-centric compilation**: Metric logic should never be constructed in a black-box. This SQL-centric approach to metric construction means that metric logic remains broadly accessible and introspectable.
- **Maximal Flexibility**: Construct any metric on any data model aggregated to any dimension. There are escape hatches, but we continually work to make them unnecessary.

## Features

Key features of MetricFlow include:

- **Metrics as Code:** MetricFlow's metric spec allows you to define a wide-range of metrics through a clean set of abstractions that encourage DRY expression of logic in YAML and SQL.
- **SQL Compilation:** Generate SQL to build metrics, without the need to repeatedly express the same joins, aggregations, filters and expressions from your data warehouse in order to construct datasets for consumption.
- **DW Connectors**: Support for data warehouse (DW) connectors give the open-source community the power to contribute to DW-specific optimizations and support. DW connectors allow users to construct metric logic to various data warehouses.
- **Command Line Interface (CLI)**: Pull data into a local context for testing and development workflows.
- **Python Library**: Pull metrics into local Python environments such as Jupyter or other analytical interfaces.
- **Materializations:** Define a set of metrics and a set of dimensions that you want to materialize to the data warehouse. This enables rapid construction of denormalized datasets back to the warehouse.

## Contributing and Code of Conduct

This project will be a place where people can easily contribute high-quality updates in a supportive environment.

You might wish to read our [code of conduct](http://community.transform.co/metricflow-signup) and <LINK> engineering practices </LINK> before diving in.

To get started on direct contributions, head on over to our [contributor guide](https://github.com/transform-data/metricflow/blob/main/CONTRIBUTING.md).

## Resources

- [Website](https://transform.co/metricflow)
- [Documentation](https://docs.transform.co/docs/overview/metricflow-overview)
- [Slack Community](https://community.transform.co/metricflow-signup)
- [MetricFlow Git Repository](https://github.com/transform-data/metricflow)
- [CHANGELOG.md](https://github.com/transform-data/metricflow/blob/main/CHANGELOG.md)

## License

MetricFlow is open source software. The project relies on several licenses including AGPL-3.0-or-later and Apache (specified at folder level).

MetricFlow is built by [Transform](https://transform.co/), the company behind the first metrics store.
