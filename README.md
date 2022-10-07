<p align="center">
  <a target="_blank" href="https://transform.co/metricflow">
    <picture>
      <img  alt="metricflow logo" src="https://github.com/transform-data/metricflow/raw/main/assets/MetricFlow_logo.png" width="auto" height="120">
    </picture>
  </a>
  Build and maintain all of your metric logic in code.
  <br /><br />
  <a target="_blank" href="https://twitter.com/transformio">
    <img src="https://img.shields.io/twitter/follow/transformio?labelColor=image.png&color=163B36&logo=twitter&style=flat">
  </a>
  <a target="_blank" href="http://community.transform.co/metricflow-signup">
    <img src="https://img.shields.io/badge/Slack-join-163B36">
  </a>
  <a target="_blank" href="https://github.com/transform-data/metricflow">
    <img src="https://img.shields.io/github/stars/transform-data/metricflow?labelColor=image.png&color=163B36&logo=github">
  </a>
  <br />
  <a target="_blank" href="https://github.com/transform-data/metricflow/blob/master/LICENSE">
    <img src="https://img.shields.io/pypi/l/metricflow?color=163B36&logo=AGPL-3.0">
  </a>
  <a target="_blank" href="https://pypi.org/project/metricflow/">
    <img src="https://img.shields.io/pypi/v/metricflow?labelColor=&color=163B36">
  </a>
  <img src="https://img.shields.io/pypi/pyversions/metricflow?labelColor=&color=163B36">
</p>

# Welcome to MetricFlow

See our latest updates in the [Metricflow Changelog](https://github.com/transform-data/metricflow/blob/main/CHANGELOG.md)!

MetricFlow is a semantic layer that makes it easy to organized metric definition and build performant and legible SQL. This makes it easy to get consistent metrics output broken down by attributes (dimensions) of interest.

The name comes from the approach taken to generate metrics. A query is compiled into a plan of nodes that represents operations(called a dataflow). The plan is then optimized and rendered to engine-specific SQL.

<p align="center">
<img src="https://github.com/transform-data/metricflow/raw/main/assets/example_plan.svg" height="500"/>
<br /><br />
</p>

MetricFlow provides a set of abstractions that help you construct complicated logic and dynamically generate queries to handle:

- Multi-hop joins between fact and dimension sources
- Complex metric types such as ratio, expression, and cumulative
- Metric aggregation to different time granularities
- And so much more

As a developer, you can also use MetricFlow's interfaces to construct APIs for integrations to bring metrics into downstream tools in your data stack.Note: You may need to install postgresql or graphviz. You can do so using brew: `brew install postgresql` or `brew install graphviz`

## Getting Started

### Install MetricFlow

MetricFlow can be installed from PyPi for use as a Python library with the following command:

```
pip install metricflow
```

Note: You may need to install postgresql or graphviz. You can do so using brew: `brew install postgresql` or `brew install graphviz`

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

The best way to get started is to follow the tutorial:

```
mf tutorial # optionally add `--skip-dw` if you have already confirmed your datawarehouse connection works
```

There are several examples of MetricFlow configs on common data sets in the [config-templates](/Users/nicholashandel/repositories/metricflow/config-templates) folder. The tutorial will rely on a small set of [sample configs](/Users/nicholashandel/repositories/metricflow/metricflow/cli/sample_models).

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

## Resources

- [Website](https://transform.co/metricflow)
- [Documentation](https://docs.transform.co/docs/overview/metricflow-overview)
- [Slack Community](https://community.transform.co/metricflow-signup)
- [MetricFlow Git Repository](https://github.com/transform-data/metricflow)
- [CHANGELOG.md](https://github.com/transform-data/metricflow/blob/main/CHANGELOG.md)
- [ROADMAP.md](https://github.com/transform-data/metricflow/blob/main/ROADMAP.md)
- [TENETS.md](https://github.com/transform-data/metricflow/blob/main/TENETS.md)

## Contributing and Code of Conduct

This project will be a place where people can easily contribute high-quality updates in a supportive environment.

You might wish to read our [code of conduct](http://community.transform.co/metricflow-signup) and <LINK> engineering practices </LINK> before diving in.

To get started on direct contributions, head on over to our [contributor guide](https://github.com/transform-data/metricflow/blob/main/CONTRIBUTING.md).

## License

MetricFlow is open source software. The project relies on several licenses including AGPL-3.0-or-later and Apache (specified at folder level).

MetricFlow is built by [Transform](https://transform.co/), the company behind the first metrics store.
