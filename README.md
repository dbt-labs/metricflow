<p align="center">
  <a target="_blank" href="https://transform.co/metricflow">
    <picture>
      <img  alt="metricflow logo" src="https://github.com/dbt-labs/metricflow/raw/main/assets/MetricFlow_logo.png" width="auto" height="120">
    </picture>
  </a>
  <br /><br />
  <b>Build and maintain all of your metric logic in code.</b>
  <br /><br />
  <a target="_blank" href="https://twitter.com/dbt_labs">
    <img src="https://img.shields.io/twitter/follow/dbt_labs?labelColor=image.png&color=163B36&logo=twitter&style=flat">
  </a>
  <a target="_blank" href="https://www.getdbt.com/community/">
    <img src="https://img.shields.io/badge/Slack-join-163B36">
  </a>
  <a target="_blank" href="https://github.com/dbt-labs/metricflow">
    <img src="https://img.shields.io/github/stars/dbt-labs/metricflow?labelColor=image.png&color=163B36&logo=github">
  </a>
  <br />
  <a target="_blank" href="https://github.com/dbt-labs/metricflow/blob/master/LICENSE">
    <img src="https://img.shields.io/pypi/l/metricflow?color=163B36&logo=AGPL-3.0">
  </a>
  <a target="_blank" href="https://pypi.org/project/metricflow/">
    <img src="https://img.shields.io/pypi/v/metricflow?labelColor=&color=163B36">
  </a>
  <img src="https://img.shields.io/pypi/pyversions/metricflow?labelColor=&color=163B36">
</p>

# Welcome to MetricFlow

See our latest updates in the [Metricflow Changelog](https://github.com/dbt-labs/metricflow/blob/main/CHANGELOG.md)!

MetricFlow is a semantic layer that makes it easy to organize metric definitions. It takes those definitions and generates legible and reusable SQL. This makes it easy to get consistent metrics output broken down by attributes (dimensions) of interest.

The name comes from the approach taken to generate metrics. A query is compiled into a query plan (represented below) called a dataflow that constructs metrics. The plan is then optimized and rendered to engine-specific SQL.

<p align="center">
<img src="https://github.com/dbt-labs/metricflow/raw/main/assets/example_plan.svg" height="500"/>
<br /><br />
</p>

MetricFlow provides a set of abstractions that help you construct complicated logic and dynamically generate queries to handle:

- Multi-hop joins between fact and dimension sources
- Complex metric types such as ratio, expression, and cumulative
- Metric aggregation to different time granularities
- And so much more

As a developer, you can also use MetricFlow's interfaces to construct APIs for integrations to bring metrics into downstream tools in your data stack.

To get up and running with your own metrics, you should rely on MetricFlow’s documentation available at [MetricFlow docs](https://docs.transform.co/docs/metricflow/guides/introduction).

## Licensing

MetricFlow is distributed under a Business Source License (BUSL-1.1). For details on our additional use grant, change license, and change date please refer to our [licensing agreement](https://github.com/dbt-labs/metricflow/blob/main/LICENSE).

## Getting Started

### Install MetricFlow

MetricFlow can be installed from PyPi for use as a Python library with the following command:

```
pip install metricflow
```

Once installed, MetricFlow can be setup and connected to a data warehouse by following the instructions after issuing the command:

```
mf setup
```

In case you don't have a connection to a data warehouse available and want a self-contained demo, DuckDB can be selected.

You may need to install Postgres or Graphviz. You can do so by following the install instructions for [Postgres](https://www.postgresql.org/download/) or [Graphviz](https://www.graphviz.org/download/). Mac users may prefer to use brew: `brew install postgresql` or `brew install graphviz`.

### Tutorial

The best way to get started is to follow the [tutorial](https://docs.transform.co/docs/metricflow/metricflow-tutorial) steps:

```
mf tutorial
```

There are several examples of MetricFlow configs on common data sets in the [config-templates](https://github.com/dbt-labs/metricflow/tree/main/config-templates) folder. The tutorial will rely on a small set of [sample configs](https://github.com/dbt-labs/metricflow/tree/main/metricflow/cli/sample_models).

## Resources

- [Website](https://transform.co/metricflow)
- [Documentation](https://docs.transform.co/docs/overview/metricflow-overview)
- [Slack Community](https://community.transform.co/metricflow-signup)
- [MetricFlow Git Repository](https://github.com/dbt-labs/metricflow)
- [CHANGELOG.md](https://github.com/dbt-labs/metricflow/blob/main/CHANGELOG.md)
- [ROADMAP.md](https://github.com/dbt-labs/metricflow/blob/main/ROADMAP.md)
- [TENETS.md](https://github.com/dbt-labs/metricflow/blob/main/TENETS.md)

## Contributing and Code of Conduct

This project will be a place where people can easily contribute high-quality updates in a supportive environment.

You might wish to read our [code of conduct](http://community.transform.co/metricflow-signup) and <LINK> engineering practices </LINK> before diving in.

To get started on direct contributions, head on over to our [contributor guide](https://github.com/dbt-labs/metricflow/blob/main/CONTRIBUTING.md).

## License

MetricFlow is source-available software.

Version 0 to 0.140.0 was covered by the Affero GPL license.
Version 0.150.0 and greater is covered by the BSL license..

MetricFlow is built by [dbt Labs](https://www.getdbt.com/).
