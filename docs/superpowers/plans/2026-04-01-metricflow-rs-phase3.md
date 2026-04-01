# MetricFlow-RS Phase 3: Joins & Dimensions

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable querying dimensions that live on different semantic models than the measure, via entity join paths. After this phase, a query like "bookings grouped by listing__country" works — where `bookings` is a measure on `bookings_source` and `country` is a dimension on `listings_source`, joined via the shared `listing` entity.

**Architecture:** Extend the semantic graph with entity edges between models. Add join path resolution to find the shortest entity link chain. Add a `JoinOnEntities` dataflow node and corresponding SQL JOIN generation.

**Tech Stack:** Same as Phase 1-2. petgraph for entity graph traversal.

**Roadmap context:** This is Phase 3 of 8. See `docs/superpowers/plans/2026-04-01-metricflow-rs-roadmap.md`. Phase 1-2 (Foundation + Simple Metrics) is complete.

---

## File Structure (changes to existing crates)

```
metricflow-rs/crates/
├── mf-core/src/
│   └── types.rs              # MODIFY: add ValidJoinPair enum
├── mf-manifest/src/
│   ├── graph.rs              # MODIFY: add entity edges, join path resolution
│   └── join_path.rs          # MODIFY: implement JoinPathResolver (was stub)
├── mf-planning/src/
│   ├── dataflow.rs           # MODIFY: add JoinOnEntities node variant
│   └── builder.rs            # MODIFY: plan joins when dimensions require them
├── mf-sql/src/
│   ├── convert.rs            # MODIFY: convert JoinOnEntities to SQL JOINs
│   └── render.rs             # (no changes needed — JOIN rendering already exists)
└── tests/fixtures/
    └── two_model_manifest.json  # NEW: test fixture with 2 models + entity link
```

---

## Task 1: Test Fixture with Two Semantic Models

**Files:**
- Create: `metricflow-rs/tests/fixtures/two_model_manifest.json`

- [ ] **Step 1: Create the fixture**

Based on MetricFlow's `sg_02_single_join` test manifest. Two semantic models connected by a `listing` entity (foreign → primary):

```json
{
  "semantic_models": [
    {
      "name": "bookings_source",
      "node_relation": {
        "alias": "fct_bookings",
        "schema_name": "demo",
        "database": null
      },
      "defaults": {
        "agg_time_dimension": "ds"
      },
      "primary_entity": "booking",
      "entities": [
        {
          "name": "booking",
          "type": "primary",
          "expr": null
        },
        {
          "name": "listing",
          "type": "foreign",
          "expr": "listing_id"
        }
      ],
      "measures": [
        {
          "name": "bookings",
          "agg": "sum",
          "expr": "1",
          "agg_time_dimension": "ds"
        }
      ],
      "dimensions": [
        {
          "name": "ds",
          "type": "time",
          "type_params": {
            "time_granularity": "day"
          },
          "expr": null
        },
        {
          "name": "is_instant",
          "type": "categorical",
          "expr": null
        }
      ]
    },
    {
      "name": "listings_source",
      "node_relation": {
        "alias": "dim_listings",
        "schema_name": "demo",
        "database": null
      },
      "primary_entity": "listing",
      "entities": [
        {
          "name": "listing",
          "type": "primary",
          "expr": "listing_id"
        }
      ],
      "measures": [],
      "dimensions": [
        {
          "name": "country",
          "type": "categorical",
          "expr": null
        },
        {
          "name": "capacity",
          "type": "categorical",
          "expr": null
        }
      ]
    }
  ],
  "metrics": [
    {
      "name": "bookings",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "bookings",
          "join_to_timespine": false
        }
      }
    }
  ],
  "project_configuration": {
    "time_spine_table_configurations": [],
    "time_spines": []
  }
}
```

- [ ] **Step 2: Verify the fixture parses**

Add a quick test in `mf-manifest/src/parse.rs`:

```rust
#[test]
fn test_parse_two_model_manifest() {
    let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
    let manifest = from_json(json).expect("should parse");
    assert_eq!(manifest.semantic_models.len(), 2);
    assert_eq!(manifest.semantic_models[0].entities.len(), 2);
    assert_eq!(manifest.semantic_models[1].entities.len(), 1);
}
```

Run: `cd metricflow-rs && cargo test -p mf-manifest --lib`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add metricflow-rs/tests/fixtures/two_model_manifest.json metricflow-rs/crates/mf-manifest/src/parse.rs
git commit -m "test: add two-model manifest fixture for join testing"
```

---

## Task 2: Entity Graph in SemanticGraph

**Files:**
- Modify: `metricflow-rs/crates/mf-manifest/src/graph.rs`

- [ ] **Step 1: Write failing test for entity-based lookups**

Add to `graph.rs` tests:

```rust
#[test]
fn test_graph_entity_links() {
    let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = SemanticGraph::build(&manifest).unwrap();

    // bookings_source has a foreign entity "listing" → should link to listings_source
    let joins = graph.find_entity_joins("bookings_source", "listing");
    assert_eq!(joins.len(), 1);
    assert_eq!(joins[0].right_model, "listings_source");
    assert_eq!(joins[0].right_entity_expr, "listing_id");
    assert_eq!(joins[0].left_entity_expr, "listing_id");
}

#[test]
fn test_graph_find_dimension_via_join() {
    let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = SemanticGraph::build(&manifest).unwrap();

    // "country" is on listings_source, reachable from bookings_source via listing entity
    let path = graph.find_join_path("bookings_source", "country", &["listing"]);
    assert!(path.is_some());
    let path = path.unwrap();
    assert_eq!(path.hops.len(), 1);
    assert_eq!(path.hops[0].join_entity, "listing");
    assert_eq!(path.hops[0].right_model, "listings_source");
}
```

Run: `cd metricflow-rs && cargo test -p mf-manifest --lib`
Expected: FAIL — methods don't exist yet

- [ ] **Step 2: Add entity join index to SemanticGraph**

Add to `graph.rs`:

```rust
/// Describes a valid join between two semantic models via a shared entity.
#[derive(Debug, Clone)]
pub struct EntityJoin<'a> {
    pub left_model: &'a str,
    pub right_model: &'a str,
    pub entity_name: &'a str,
    pub left_entity_expr: String,   // SQL expression for the join key on the left
    pub right_entity_expr: String,  // SQL expression for the join key on the right
}

/// A single hop in a join path.
#[derive(Debug, Clone)]
pub struct JoinHop {
    pub join_entity: String,
    pub right_model: String,
    pub left_entity_expr: String,
    pub right_entity_expr: String,
}

/// A complete join path from a source model to a target dimension.
#[derive(Debug, Clone)]
pub struct JoinPath {
    pub hops: Vec<JoinHop>,
    pub target_model: String,
}
```

In `SemanticGraph::build()`, after the existing index construction, add a new index:

```rust
/// Maps (left_model_name, entity_name) → list of valid EntityJoins.
/// A valid join is: left model has entity as FOREIGN/UNIQUE, right model has it as PRIMARY/UNIQUE.
entity_joins: HashMap<(&'a str, &'a str), Vec<EntityJoin<'a>>>,
```

Build it by iterating all pairs of semantic models. For each pair, check if they share an entity name where one side is foreign and the other is primary (or both unique). Valid pairs:
- `FOREIGN → PRIMARY`
- `FOREIGN → UNIQUE`
- `UNIQUE → PRIMARY`
- `UNIQUE → UNIQUE`

Add methods:

```rust
/// Find valid entity joins from a model via a specific entity.
pub fn find_entity_joins(&self, model_name: &str, entity_name: &str) -> Vec<&EntityJoin<'a>>

/// Find the join path from a source model to a dimension, given the entity path.
/// entity_path is the list of entity names to traverse (e.g., ["listing"]).
pub fn find_join_path(
    &self,
    source_model: &str,
    dim_name: &str,
    entity_path: &[&str],
) -> Option<JoinPath>
```

`find_join_path` walks the entity path one hop at a time:
1. Start at `source_model`
2. For each entity in `entity_path`, look up `entity_joins[(current_model, entity)]`
3. Pick the first valid join, advance to the right model
4. After all hops, verify the target model has the requested dimension

- [ ] **Step 3: Run tests**

Run: `cd metricflow-rs && cargo test -p mf-manifest --lib`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add metricflow-rs/crates/mf-manifest/src/graph.rs
git commit -m "feat(mf-manifest): add entity join index and join path resolution to SemanticGraph"
```

---

## Task 3: JoinOnEntities Dataflow Node

**Files:**
- Modify: `metricflow-rs/crates/mf-planning/src/dataflow.rs`

- [ ] **Step 1: Write test for JoinOnEntities node**

Add to `dataflow.rs` tests:

```rust
#[test]
fn test_join_plan_structure() {
    let mut plan = DataflowPlan::new();

    let left = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let right = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "listings_source".into(),
        table: "demo.dim_listings".into(),
    });
    let join = plan.add_node(DataflowNode::JoinOnEntities {
        join_type: JoinType::LeftOuter,
        left_entity_expr: "listing_id".into(),
        right_entity_expr: "listing_id".into(),
        right_model_alias: "listings_source".into(),
    });
    plan.add_edge(left, join);
    plan.add_edge(right, join);
    plan.set_sink(join);

    assert_eq!(plan.node_count(), 3);
    let parents = plan.parents(join);
    assert_eq!(parents.len(), 2);
}
```

- [ ] **Step 2: Add JoinOnEntities variant to DataflowNode**

```rust
// In DataflowNode enum, add:
JoinOnEntities {
    join_type: JoinType,
    left_entity_expr: String,   // e.g., "listing_id"
    right_entity_expr: String,  // e.g., "listing_id"
    right_model_alias: String,  // for SQL aliasing
},
```

This requires importing `JoinType` from `mf_core::types`.

- [ ] **Step 3: Run tests**

Run: `cd metricflow-rs && cargo test -p mf-planning --lib`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add metricflow-rs/crates/mf-planning/src/dataflow.rs
git commit -m "feat(mf-planning): add JoinOnEntities dataflow node variant"
```

---

## Task 4: Plan Builder — Join Support

**Files:**
- Modify: `metricflow-rs/crates/mf-planning/src/builder.rs`

- [ ] **Step 1: Write failing test for join plan**

Add to `builder.rs` tests:

```rust
#[test]
fn test_build_plan_with_entity_join() {
    let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![
            GroupBySpec::Dimension {
                name: "country".into(),
                entity_path: vec!["listing".into()],
            },
        ],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let plan = build_plan(&graph, &query).unwrap();
    let sink = plan.sink().unwrap();

    // Should be: Aggregate with parent JoinOnEntities
    match plan.node(sink) {
        DataflowNode::Aggregate { group_by, aggregations } => {
            assert!(group_by.contains(&"listing__country".to_string()));
            assert_eq!(aggregations.len(), 1);
            assert_eq!(aggregations[0].measure_name, "bookings");
        }
        other => panic!("expected Aggregate, got {other:?}"),
    }

    // The aggregate's parent should be a JoinOnEntities node
    let agg_parents = plan.parents(sink);
    assert_eq!(agg_parents.len(), 1);
    match plan.node(agg_parents[0]) {
        DataflowNode::JoinOnEntities { join_type, .. } => {
            assert_eq!(*join_type, JoinType::LeftOuter);
        }
        other => panic!("expected JoinOnEntities, got {other:?}"),
    }

    // The join node should have 2 parents: left (bookings) and right (listings)
    let join_parents = plan.parents(agg_parents[0]);
    assert_eq!(join_parents.len(), 2);
}

#[test]
fn test_build_plan_mixed_local_and_join_dimensions() {
    let json = include_str!("../../../tests/fixtures/two_model_manifest.json");
    let manifest = parse::from_json(json).unwrap();
    let graph = mf_manifest::graph::SemanticGraph::build(&manifest).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![
            GroupBySpec::Dimension {
                name: "is_instant".into(),
                entity_path: vec![],
            },
            GroupBySpec::Dimension {
                name: "country".into(),
                entity_path: vec!["listing".into()],
            },
        ],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let plan = build_plan(&graph, &query).unwrap();
    let sink = plan.sink().unwrap();

    match plan.node(sink) {
        DataflowNode::Aggregate { group_by, .. } => {
            assert!(group_by.contains(&"is_instant".to_string()));
            assert!(group_by.contains(&"listing__country".to_string()));
        }
        other => panic!("expected Aggregate, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd metricflow-rs && cargo test -p mf-planning --lib`
Expected: FAIL

- [ ] **Step 3: Update build_plan to handle joins**

Modify `build_plan` in `builder.rs`. The key logic change:

1. Separate `group_by` specs into local (empty entity_path) and remote (non-empty entity_path)
2. If all are local, use the existing code path (no join)
3. If any are remote:
   a. Build `ReadFromSource` for the measure's model (left)
   b. For each unique entity in the remote specs, resolve the join path via `graph.find_join_path()`
   c. Build `ReadFromSource` for each right-side model
   d. Build `JoinOnEntities` node connecting left and right, with edges from both reads
   e. Build `Aggregate` on top of the join node

```rust
pub fn build_plan(graph: &SemanticGraph, query: &QuerySpec) -> Result<DataflowPlan, PlanError> {
    if query.metrics.len() != 1 {
        return Err(PlanError::UnsupportedMetricType);
    }

    let metric_name = &query.metrics[0];
    let resolved = resolve::resolve_simple_metric(graph, metric_name)?;
    let mut plan = DataflowPlan::new();

    // Separate local vs remote dimensions
    let (local_dims, remote_dims): (Vec<_>, Vec<_>) = query.group_by.iter().partition(|g| {
        match g {
            GroupBySpec::Dimension { entity_path, .. }
            | GroupBySpec::TimeDimension { entity_path, .. }
            | GroupBySpec::Entity { entity_path, .. } => entity_path.is_empty(),
        }
    });

    let read_left = plan.add_node(DataflowNode::ReadFromSource {
        model_name: resolved.model.name.clone(),
        table: resolved.model.node_relation.fully_qualified(),
    });

    let source_for_aggregate = if remote_dims.is_empty() {
        // No joins needed — same as Phase 1-2
        read_left
    } else {
        // Group remote dims by entity path (first entity in path)
        // For Phase 3: support single-hop joins (entity_path length == 1)
        let mut join_node = read_left;

        // Collect unique entities to join
        let mut seen_entities: Vec<String> = vec![];
        for dim in &remote_dims {
            let entity = match dim {
                GroupBySpec::Dimension { entity_path, .. }
                | GroupBySpec::TimeDimension { entity_path, .. }
                | GroupBySpec::Entity { entity_path, .. } => {
                    entity_path.first().ok_or(PlanError::UnsupportedMetricType)?
                }
            };
            if !seen_entities.contains(entity) {
                seen_entities.push(entity.clone());
            }
        }

        for entity_name in &seen_entities {
            let entity_refs: Vec<&str> = vec![entity_name.as_str()];
            // Use first remote dim that uses this entity to find the target model
            let dim_name = remote_dims.iter().find_map(|d| match d {
                GroupBySpec::Dimension { name, entity_path, .. }
                    if entity_path.first().map(|s| s.as_str()) == Some(entity_name.as_str()) =>
                {
                    Some(name.as_str())
                }
                _ => None,
            }).ok_or(PlanError::UnsupportedMetricType)?;

            let join_path = graph
                .find_join_path(&resolved.model.name, dim_name, &entity_refs)
                .ok_or_else(|| PlanError::Resolve(ResolveError::DimensionNotFound(
                    dim_name.into(),
                    resolved.model.name.clone(),
                )))?;

            for hop in &join_path.hops {
                let right_model = graph.find_model(&hop.right_model)
                    .ok_or_else(|| PlanError::Resolve(ResolveError::NoModelForMeasure(hop.right_model.clone())))?;

                let read_right = plan.add_node(DataflowNode::ReadFromSource {
                    model_name: right_model.name.clone(),
                    table: right_model.node_relation.fully_qualified(),
                });

                let new_join = plan.add_node(DataflowNode::JoinOnEntities {
                    join_type: JoinType::LeftOuter,
                    left_entity_expr: hop.left_entity_expr.clone(),
                    right_entity_expr: hop.right_entity_expr.clone(),
                    right_model_alias: right_model.name.clone(),
                });

                plan.add_edge(join_node, new_join);
                plan.add_edge(read_right, new_join);
                join_node = new_join;
            }
        }

        join_node
    };

    // Build group-by column names and aggregation (same as Phase 1-2)
    let group_by_columns: Vec<String> = query.group_by.iter().map(|g| g.column_name()).collect();

    let aggregations = vec![MeasureAggregation {
        measure_name: resolved.measure.name.clone(),
        agg_type: resolved.measure.agg,
        expr: resolved.measure.sql_expr().to_string(),
        alias: resolved.metric.name.clone(),
    }];

    let agg_node = plan.add_node(DataflowNode::Aggregate {
        group_by: group_by_columns,
        aggregations,
    });
    plan.add_edge(source_for_aggregate, agg_node);

    // Optional ORDER BY / LIMIT (same as Phase 1-2)
    let mut current = agg_node;
    if !query.order_by.is_empty() {
        let order_specs: Vec<(String, bool)> = query
            .order_by
            .iter()
            .map(|o| (o.column.column_name(), o.descending))
            .collect();
        let order_node = plan.add_node(DataflowNode::OrderBy { specs: order_specs });
        plan.add_edge(current, order_node);
        current = order_node;
    }
    if let Some(count) = query.limit {
        let limit_node = plan.add_node(DataflowNode::Limit { count });
        plan.add_edge(current, limit_node);
        current = limit_node;
    }

    plan.set_sink(current);
    Ok(plan)
}
```

- [ ] **Step 4: Run tests**

Run: `cd metricflow-rs && cargo test -p mf-planning --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-planning/src/builder.rs
git commit -m "feat(mf-planning): support entity joins in plan builder"
```

---

## Task 5: SQL Converter — JOIN Generation

**Files:**
- Modify: `metricflow-rs/crates/mf-sql/src/convert.rs`

- [ ] **Step 1: Write failing test**

Add to `convert.rs` tests:

```rust
#[test]
fn test_convert_join_to_sql() {
    let mut plan = DataflowPlan::new();

    let left = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "bookings_source".into(),
        table: "demo.fct_bookings".into(),
    });
    let right = plan.add_node(DataflowNode::ReadFromSource {
        model_name: "listings_source".into(),
        table: "demo.dim_listings".into(),
    });
    let join = plan.add_node(DataflowNode::JoinOnEntities {
        join_type: JoinType::LeftOuter,
        left_entity_expr: "listing_id".into(),
        right_entity_expr: "listing_id".into(),
        right_model_alias: "listings_source".into(),
    });
    plan.add_edge(left, join);
    plan.add_edge(right, join);

    let agg = plan.add_node(DataflowNode::Aggregate {
        group_by: vec!["listing__country".into()],
        aggregations: vec![MeasureAggregation {
            measure_name: "bookings".into(),
            agg_type: AggregationType::Sum,
            expr: "1".into(),
            alias: "bookings".into(),
        }],
    });
    plan.add_edge(join, agg);
    plan.set_sink(agg);

    let sql = to_sql_standalone(&plan).unwrap();

    // The SQL should contain a JOIN
    let renderer = crate::render::DefaultRenderer;
    let rendered = renderer.render(&sql);
    assert!(rendered.contains("LEFT OUTER JOIN"), "should have LEFT OUTER JOIN: {rendered}");
    assert!(rendered.contains("listing_id"), "should join on listing_id: {rendered}");
    assert!(rendered.contains("GROUP BY"), "should have GROUP BY: {rendered}");
    eprintln!("Generated SQL:\n{rendered}");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd metricflow-rs && cargo test -p mf-sql --lib`
Expected: FAIL — `JoinOnEntities` not handled in `convert_node`

- [ ] **Step 3: Implement JoinOnEntities conversion**

In `convert.rs`, add a handler for `JoinOnEntities` in `convert_node`:

```rust
DataflowNode::JoinOnEntities {
    join_type,
    left_entity_expr,
    right_entity_expr,
    right_model_alias,
} => {
    let parents = plan.parents(node_idx);
    // Parents are [left, right] (order depends on insertion, but left is first edge added)
    // We need to identify which parent is left vs right
    let left_sql = convert_node(plan, parents[0], subquery_counter)?;
    let right_sql = convert_node(plan, parents[1], subquery_counter)?;

    let left_alias = format!("subq_{subquery_counter}");
    *subquery_counter += 1;
    let right_alias = format!("{right_model_alias}_src");

    let join_type_str = match join_type {
        JoinType::LeftOuter => "LEFT OUTER JOIN",
        JoinType::Inner => "INNER JOIN",
        JoinType::FullOuter => "FULL OUTER JOIN",
        JoinType::CrossJoin => "CROSS JOIN",
    };

    Ok(SqlSelect {
        select_columns: vec![SqlExpr::Literal("*".into())],
        from: SqlFrom::Subquery {
            query: Box::new(left_sql),
            alias: left_alias.clone(),
        },
        joins: vec![SqlJoin {
            join_type: join_type_str.into(),
            source: SqlFrom::Subquery {
                query: Box::new(right_sql),
                alias: right_alias.clone(),
            },
            on: SqlExpr::Literal(format!(
                "{left_alias}.{left_entity_expr} = {right_alias}.{right_entity_expr}"
            )),
        }],
        where_clause: None,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    })
}
```

Also update the `convert_aggregate` function to handle the case where the source is a join (it now has `SELECT *` and joins, not a bare table). The inner select for aggregate should reference the join alias.

- [ ] **Step 4: Run tests**

Run: `cd metricflow-rs && cargo test -p mf-sql --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add metricflow-rs/crates/mf-sql/src/convert.rs
git commit -m "feat(mf-sql): generate SQL JOINs from JoinOnEntities dataflow node"
```

---

## Task 6: End-to-End Join Integration Test

**Files:**
- Modify: `metricflow-rs/tests/integration.rs`

- [ ] **Step 1: Write integration test**

```rust
#[test]
fn test_end_to_end_join_dimension() {
    let manifest_json = include_str!("fixtures/two_model_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![GroupBySpec::Dimension {
            name: "country".into(),
            entity_path: vec!["listing".into()],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    assert!(sql.contains("LEFT OUTER JOIN"), "should have JOIN: {sql}");
    assert!(sql.contains("listing_id"), "should join on entity key: {sql}");
    assert!(sql.contains("listing__country"), "should have prefixed dimension: {sql}");
    assert!(sql.contains("SUM"), "should aggregate: {sql}");
    assert!(sql.contains("GROUP BY"), "should group: {sql}");
    assert!(sql.contains("demo.fct_bookings"), "should reference left table: {sql}");
    assert!(sql.contains("demo.dim_listings"), "should reference right table: {sql}");

    eprintln!("Generated SQL:\n{sql}");
}

#[test]
fn test_end_to_end_mixed_local_and_join_dimensions() {
    let manifest_json = include_str!("fixtures/two_model_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![
            GroupBySpec::Dimension {
                name: "is_instant".into(),
                entity_path: vec![],
            },
            GroupBySpec::Dimension {
                name: "country".into(),
                entity_path: vec!["listing".into()],
            },
        ],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    assert!(sql.contains("is_instant"), "should have local dim: {sql}");
    assert!(sql.contains("listing__country"), "should have joined dim: {sql}");
    assert!(sql.contains("LEFT OUTER JOIN"), "should have JOIN: {sql}");

    eprintln!("Generated SQL:\n{sql}");
}
```

- [ ] **Step 2: Run integration tests**

Run: `cd metricflow-rs && cargo test --test integration`
Expected: all 5 tests pass (3 old + 2 new)

- [ ] **Step 3: Commit**

```bash
git add metricflow-rs/tests/integration.rs
git commit -m "test: add end-to-end integration tests for entity joins"
```

---

## Task 7: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `cd metricflow-rs && cargo test --all --lib && cargo test --test integration`
Expected: all tests pass

- [ ] **Step 2: Run clippy and fmt**

Run: `cd metricflow-rs && cargo clippy --all -- -D warnings && cargo fmt --all -- --check`
Expected: clean

- [ ] **Step 3: Fix any issues and commit**

```bash
cargo fmt --all
git add -A && git commit -m "style: fix formatting"
```

---

## What This Plan Produces

After Phase 3, the pipeline handles:
- Simple metrics with dimensions on the **same** semantic model (Phase 1-2)
- Simple metrics with dimensions on a **different** semantic model, joined via entity links (Phase 3)
- Single-hop joins (one entity link, e.g., `listing__country`)
- Mixed queries with both local and joined dimensions

## Known Limitations (for future phases)

- Multi-hop joins (2+ entity links, e.g., `listing__user__country`) — supported in the data model but not yet tested. The `find_join_path` implementation handles it, but the builder only processes one hop at a time.
- Partition join predicates (e.g., joining on both entity key AND date) — not implemented.
- Right-side column selection (currently `SELECT *` on joined tables) — optimization deferred to Phase 8.
