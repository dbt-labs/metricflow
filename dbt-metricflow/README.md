<p align="center">
    <a target="_blank" href="https://twitter.com/dbt_labs">
    <img src="https://img.shields.io/twitter/follow/dbt_labs?labelColor=image.png&color=163B36&logo=twitter&style=flat">
  </a>
    <a target="_blank" href="https://www.getdbt.com/community/">
    <img src="https://img.shields.io/badge/Slack-join-163B36">
  </a>
    <a href="https://github.com/psf/black"><img src="https://img.shields.io/badge/code%20style-black-000000.svg" /></a>
</p>

# Welcome to dbt-metricflow

This repo encapsulates the dbt-core, MetricFlow, and supported dbt-adapters packages. This package will manage the versioning between these packages such that they are compatible with each other.

## Repo use cases
- dbt-core and MetricFlow both depend on dbt-semantic-interfaces, which includes the schemas and interfaces for objects related to the semantic layer. Bundled versioning is necessary to ensure that the dbt-core and MetricFlow versions have compatible dbt-semantic-interfaces dependencies.
- Bundling the installation makes it much easier on end users, as they no longer need to install `dbt-core` + `metricflow` + `dbt-adapter` and reconcile versions - instead they can simply install `dbt-metricflow[adapter]`.
- Because this encapsulates both dbt-core and MetricFlow, this repo can be used to build logic that should be shared across the packages. For example, the CLI from MetricFlow can live in this repo, as it uses logic from all of the bundled packages. This can streamline dependency requirements in MetricFlow.
