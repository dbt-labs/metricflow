# Security Policy

## Reporting a Vulnerability

If you believe you have found a security vulnerability in MetricFlow, we encourage you to report it responsibly. **Please do not open a public GitHub issue for security vulnerabilities.**

### How to Report

Use GitHub's built-in private vulnerability reporting to submit your report:

1. Navigate to the **Security** tab of this repository.
2. Click **"Report a vulnerability"** under **Private vulnerability reporting**.
3. Fill out the advisory form with as much detail as possible.

Alternatively, you can go directly to:

```
https://github.com/dbt-labs/metricflow/security/advisories/new
```

### What to Include

To help us understand and resolve the issue quickly, please include:

- A description of the vulnerability and its potential impact.
- Step-by-step instructions to reproduce the issue.
- The affected version(s) of MetricFlow.
- Any relevant logs, screenshots, or proof-of-concept code.
- Your suggested severity (Critical, High, Medium, Low) if you have one.

### What to Expect

- **Acknowledgment:** We will acknowledge receipt of your report within **3 business days**.
- **Updates:** We will provide status updates as we investigate, typically within **10 business days**.
- **Resolution:** Once a fix is ready, we will coordinate disclosure with you before making any public announcement.

### Scope

This policy applies to the latest supported release of MetricFlow. If you are unsure whether a version is still supported, please include the version details in your report and we will let you know.

MetricFlow handles database connections and generates SQL queries, so potential security concerns include but are not limited to:

- SQL injection vectors in generated queries.
- Credential exposure through configuration or logging.
- Unauthorized access to data warehouse connections.

### Guidelines

We ask that you:

- Give us a reasonable amount of time to address the issue before making any public disclosure.
- Make a good-faith effort to avoid disrupting the project, its infrastructure, or its users during your research.
- Do not access, modify, or delete data that does not belong to you.

## Supported Versions

| Version | Supported |
| ------- | --------- |
| Latest  | Yes       |

For questions about which versions are actively maintained, please refer to the project's [release documentation](https://github.com/dbt-labs/metricflow/releases).

## Thank You

We appreciate the security research community's efforts in helping keep dbt Labs and our users safe. Responsible disclosure makes the open-source ecosystem stronger for everyone.

For urgent or critical issues, please reach out to the [dbt Labs Security Team](mailto:security+ghreport@dbtlabs.com).
