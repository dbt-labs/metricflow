# Glossary

## Persistent source schema
Many tests generate and execute SQL that depend on tables containing test data. By default, a
pytest fixture creates a temporary schema and populates it with the tables that are required by
the tests. This schema is referred to the source schema. Creating the source schema (and
the associated tables) can be a slow process for some SQL engines. Since these tables generally
do not change often, functionality was added to use a source schema that is assumed to already
exist when running tests and persists between runs (a persistent source schema). In addition,
functionality was added to create the persistent source schema based on table definitions in the
repo. Because the name of the source schema is generated based on the hash of the data that's
supposed to be in the schema, the creating and populating the persistent source schema should
not be done concurrently as there are race conditions when creating tables and inserting data.
