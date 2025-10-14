# MetricFlow Governance

MetricFlow is an open-source project licensed under the Apache License, Version 2.0. MetricFlow is developed and maintained as part of the [Open Semantic Interchange (OSI) initiative](https://www.snowflake.com/en/blog/open-semantic-interchange-ai-standard/). The OSI initiative is a collaborative effort among leading technology companies and data practitioners to establish an open standard for defining and exchanging semantic information for analytics, enabling AI/BI interoperability across different platforms and tools. MetricFlow will be maintained as a collaborative and community-driven project to power the mission of analytic metadata interoperability.

## Roles and Responsibilities

### Users

Individuals or organizations who use MetricFlow and provide feedback. Users help identify bugs, suggest improvements, and participate in community discussions. Join the Community Slack to join the conversation in the #metricflow channel.

### Contributors

Anyone who contributes to the project — through code, documentation, design, testing, or community support. All contributors operate under the ALv2 license and retain authorship credit. For more information about contributing, see `CONTRIBUTING.md`.

### Committers

Contributors who have earned the community’s trust and are granted write access to the repository.
Committers review pull requests, merge approved changes, and maintain releases.

### Maintainers

Overall project direction will be overseen by a small group of maintainers composed of active committers and representatives from the founding OSI organizations, including dbt Labs, Snowflake, and Tableau. This group is responsible for:

- Setting the project vision and roadmap for semantic metadata interoperability
- Approving major changes and releases
- Ensuring the project adheres to its open-source principles and community guidelines

Initial maintainers include, but are not limited to:

- [Paul Yang](https://github.com/plypaul), dbt Labs* 
- [Courtney Holcomb](https://github.com/courtneyholcomb), dbt Labs
- [Khushboo Bhatia](https://github.com/khush-bhatia), Snowflake
- [Zerui Wei](https://github.com/wzrumich), Snowflake
- 2 contributors from Tableau to be named later. 

* denotes technical lead

Maintainers will have equal say over the metadata representations that power semantic interoperability (see repository structure section below) per the goals of OSI. Technical leads maintain priority control over technical / code-related decisions. 

This governance model will evolve as the OSI project matures and additional organizations join the initiative.

## Repository Structure 

The MetricFlow repository is organized to facilitate collaboration on an open standard for semantics. Key directories include:

| Directory | Purpose |
|------------|----------|
| `metricflow-semantic-interfaces/` | Houses the semantic metadata representations to enable interoperability between OSI organizations' platforms. This will be evolved and extended by OSI partners to meet the needs of the community and industry. |
| `metricflow/` | Contains the source code for the MetricFlow engine that can compile metric SQL queries based on metricflow-semantic-interfaces metadata. |


## Contributing 

Community contributions are welcome! Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines on how to contribute to the project. Issues and feature requests can be submitted via the GitHub Issues page. Please ensure that you follow the issue template and provide as much detail as possible.

## Code of Conduct

MetricFlow emulates the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html) as a model.
All community members are expected to engage respectfully and inclusively.

*Last updated: 2025-10-14*
