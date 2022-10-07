Design of interfaces relies on a set of core tenets:

- **DRY (Don’t Repeat Yourself)**: This principle is the core objective of the underlying MetricFlow spec. Duplication of logic leads to incorrectly constructed metrics and should be avoided through thoughtfully-designed abstractions.
- **SQL-centric compilation**: Metric logic should never be constructed in a black-box. This SQL-centric approach to metric construction means that metric logic remains broadly accessible and introspectable.
- **Maximal Flexibility**: Construct any metric on any data model aggregated to any dimension. There are escape hatches, but we continually work to make them unnecessary.

Construction of SQL relies on a set of ordered priorities:

1. **Get the metric right**: Above all else, we prioritize getting the logic correct.
1. **Performance**: The logic should be performant when executed against the respective compute platforms.
1. **Legibility**: The logic should be legible and easy for an analyst to read.
1. **Ease of Manipulation**: The code to generate logic should be easy to work with and manipulatable.
