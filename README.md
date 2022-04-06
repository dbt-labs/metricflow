<p align="center">
<img src="assets/MetricFlow_logo.png" />
<br /><br />
</p>

## Welcome to MetricFlow

MetricFlow is a computational framework for building and maintaining consistent metric logic. The name comes from the approach taken to generate metrics. Using the user-defined semantic model, a query is first compiled into a metric dataflow plan. The plan is then converted to an abstract SQL object model, optimized, and rendered to engine-specific SQL.

MetricFlow provides a set of abstractions that help you construct complicated logic and dynamically generate queries to handle:

* Complex metric types such as ratio, expression, and cumulative.
* Multi-hop joins between fact and dimension sources.
* Metric aggregation to different time granularities.

As a developer, you can also use MetricFlow's interfaces to construct APIs for integrations to bring metrics into downstream tools in your data stack.

MetricFlow itself acts as a semantic layer, compiling the semantic information described in the MetricFlow spec to SQL that can be executed against the data warehouse and served to downstream applications. It acts as a proxy, translating metric requests in the form of “metrics by dimensions” into SQL queries that traverse the data warehouse and the underlying semantic structure to resolve every possible combination of metric and dimension.

### Core Tenets

The framework relies on a set of core tenets:

- **DRY (Don’t Repeat Yourself)**: This principle is the core objective of the underlying MetricFlow spec. Duplication of logic leads to incorrectly constructed metrics and should be avoided through thoughtfully-designed abstractions.
- **SQL-centric compilation**: Metric logic should never be constructed in a black-box. This SQL-centric approach to metric construction means that metric logic remains broadly accessible and introspectable.
- **Maximal Flexibility**: Construct any metric on any data model aggregated to any dimension. There are escape hatches, but we continually work to make them unnecessary.

### Features

Key features of MetricFlow include:

- **Metrics as Code:** MetricFlow's metric spec allows you to define a wide-range of metrics through a clean set of abstractions that encourage DRY expression of logic in YAML and SQL.
- **SQL Compilation:** Generate SQL to build metrics, without the need to repeatedly express the same joins, aggregations, filters and expressions from your data warehouse in order to construct datasets for consumption.
- **DW Connectors**: Support for data warehouse (DW) connectors give the open-source community the power to contribute to DW-specific optimizations and support. DW connectors allow users to construct metric logic to various data warehouses.
- **Command Line Interface (CLI)**: Pull data into a local context for testing and development workflows.
- **Python Library**: Pull metrics into local Python environments such as Jupyter or other analytical interfaces.
- **Materializations:** Define a set of metrics and a set of dimensions that you want to materialize to the data warehouse. This enables rapid construction of denormalized datasets back to the warehouse.
- **Accessible interfaces**: Construct APIs and SDKs so you can pull metric datasets into downstream applications.
  - _React Components_ to build embedded analytics
  - _Airflow Operators_ to schedule API requests and pre-construction of metrics
  - _GraphQL interface_ for the end-user to build their own interfaces for metrics

## Documentation and Getting Started

MetricFlow documentation can be found on the [Transform Documentation site](https://docs.transform.co/docs/overview/metricflow-overview). If you’re new to MetricFlow, we suggest that you review our [Getting Started](https://docs.transform.co/docs/metricflow/getting-started) section and [tutorial](https://docs.transform.co/docs/metricflow/metricflow-tutorial)

Additional resources:

Website: https://transform.co/metricflow

Documentation: https://docs.transform.co/docs/overview/metricflow-overview

Slack Community: https://community.transform.co/metricflow-signup

Main repository: https://github.com/transform-data/metricflow

Changelog: [CHANGELOG.md](CHANGELOG.md)

## Install MetricFlow

MetricFlow can be installed from PyPi for use as a Python library with the following command:

`pip install metricflow`

Once installed, MetricFlow can be setup and connected to a data warehouse by following the instructions after issuing the command:

`mf setup`

To see what MetricFlow can do without custom configurations, start the tutorial by running:

`mf tutorial`

To get up and running with your own metrics, you should rely on MetricFlow’s documentation available at [MetricFlow docs](https://docs.transform.co/docs/metricflow/guides/introduction).

## Contributing and Code of Conduct

This project will be a place where people can easily contribute high-quality updates in a supportive environment.

You might wish to read our [code of conduct](http://community.transform.co/metricflow-signup) and <LINK> engineering practices </LINK> before diving in.

To get started on direct contributions, head on over to our [contributor guide](CONTRIBUTING.md)

## License

MetricFlow is open source software. The project relies on several licenses including AGPL-3.0-or-later and Apache (specified at folder level).

MetricFlow is built by [Transform](https://transform.co/), the company behind the first metrics store.
