name: ☣️ Regression
description: Report a regression you've observed in a newer version of MetricFlow
title: "[Regression] <title>"
labels: ["bug", "regression", "triage"]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for taking the time to fill out this regression report!
  - type: checkboxes
    attributes:
      label: Is this a regression in a recent version of MetricFlow?
      description: >
        A regression is when documented functionality works as expected in an older version of MetricFlow, but no longer works after upgrading to a newer version of MetricFlow.
      options:
        - label: I believe this is a regression in MetricFlow functionality
          required: true
        - label: I have searched the existing issues, and I could not find an existing issue for this regression
          required: true
  - type: textarea
    attributes:
      label: Current Behavior
      description: A concise description of what you're experiencing.
    validations:
      required: true
  - type: textarea
    attributes:
      label: Expected/Previous Behavior
      description: A concise description of what you expected to happen.
    validations:
      required: true
  - type: textarea
    attributes:
      label: Steps To Reproduce
      description: Steps to reproduce the behavior.
      placeholder: |
        1. In this environment...
        2. With this config...
        3. Run '...'
        4. See error...
    validations:
      required: true
  - type: textarea
    id: logs
    attributes:
      label: Relevant log output
      description: |
        If applicable, log output to help explain your problem.
      render: shell
    validations:
      required: false
  - type: textarea
    attributes:
      label: Environment
      description: |
        examples:
          - **OS**: Ubuntu 20.04
          - **Python**: 3.9.12 (`python3 --version`)
          - **MetricFlow (working version)**: 0.15.0 
          - **MetricFlow (regression version)**: 0.14.1
      value: |
        - OS:
        - Python:
        - MetricFlow (working version):
        - MetricFlow (regression version):
      render: markdown
    validations:
      required: true
  - type: dropdown
    id: database
    attributes:
      label: Which database are you using with MetricFlow?
      multiple: true
      options:
        - postgres
        - redshift
        - snowflake
        - bigquery
        - other (mention it in "Additional Context")
    validations:
      required: false
  - type: textarea
    attributes:
      label: Additional Context
      description: |
        Links? References? Anything that will give us more context about the issue you are encountering!

        Tip: You can attach images or log files by clicking this area to highlight it and then dragging files in.
    validations:
      required: false