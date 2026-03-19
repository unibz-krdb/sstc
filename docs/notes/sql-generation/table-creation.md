# Table Creation

The compiler generates a family of tables for every relational algebra table definition in both source and target contexts. Beyond the base table itself, each table spawns INSERT/DELETE tracking tables, JOIN staging tables, and associated constraint enforcement functions. A single `_LOOP` table shared across the entire schema provides the cycle-prevention mechanism that coordinates the trigger chain.

---

## Base table

Each table definition produces a `CREATE TABLE` with typed columns, a `PRIMARY KEY` constraint, and zero or more `FOREIGN KEY` constraints referencing other tables in the same schema side.

```sql
CREATE TABLE schema._tablename (
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    col3 VARCHAR(100),
    PRIMARY KEY (col1),
    FOREIGN KEY (col2) REFERENCES schema._other_table (col2)
);
```

Concrete example from the source schema:

```sql
CREATE TABLE transducer._empdep (
    ssn VARCHAR(100),
    name VARCHAR(100),
    phone VARCHAR(100),
    email VARCHAR(100),
    dep_name VARCHAR(100),
    dep_address VARCHAR(100),
    PRIMARY KEY (ssn, phone, email),
    FOREIGN KEY (dep_address) REFERENCES transducer._position (dep_address)
);
```

Concrete example from the target schema:

```sql
CREATE TABLE transducer._person (
    ssn VARCHAR(100),
    name VARCHAR(100),
    dep_name VARCHAR(100),
    PRIMARY KEY (ssn),
    FOREIGN KEY (dep_name) REFERENCES transducer._department (dep_name)
);
```

## Column types

All columns currently use `VARCHAR(100)`. Types are derived from the universal schema JSON file, which defines each attribute as `{name, data_type, is_nullable}`. The compiler reads these definitions and maps them to PostgreSQL column types in the generated DDL.

## INSERT tracking table

An empty clone of the base table, created using `SELECT * ... WHERE 1<>1` so it has the same column schema but no rows. Receives copies of newly-inserted rows via the base INSERT trigger.

```sql
CREATE TABLE schema._tablename_INSERT AS
SELECT * FROM schema._tablename
WHERE 1<>1;
```

## INSERT_JOIN table

Same pattern as the INSERT tracking table. Used as a staging area during the JOIN phase of the trigger chain, where rows are projected from the universal temp table into per-table _INSERT_JOIN tables.

```sql
CREATE TABLE schema._tablename_INSERT_JOIN AS
SELECT * FROM schema._tablename
WHERE 1<>1;
```

## DELETE tracking and DELETE_JOIN tables

These follow the exact same `SELECT * ... WHERE 1<>1` pattern as their INSERT counterparts. The DELETE tracking table receives copies of deleted rows (via `OLD` instead of `NEW`), and the DELETE_JOIN table stages rows during the delete propagation chain.

```sql
CREATE TABLE schema._tablename_DELETE AS
SELECT * FROM schema._tablename
WHERE 1<>1;

CREATE TABLE schema._tablename_DELETE_JOIN AS
SELECT * FROM schema._tablename
WHERE 1<>1;
```

## The _LOOP table

A single shared table across the entire transducer schema. Stores integer markers that coordinate the trigger chain and prevent infinite loops between source and target sides.

```sql
CREATE TABLE schema._LOOP (loop_start INT NOT NULL);
```

Source-side triggers insert `1` into `_LOOP`; target-side triggers insert `-1`. The mapping functions check whether all loop markers have the same absolute value (meaning all tables on one side have fired) before proceeding. See [insert-chain.md](insert-chain.md) for the full mechanism.

For multi-tuple DELETE transactions, `_LOOP` is pre-seeded with a count before the DELETEs begin (e.g., `INSERT INTO _loop VALUES (4)` before deleting from 4 tables). See [delete-chain.md](delete-chain.md) for details.

## Per-table generation checklist

For every table `T` in either source or target schema, the compiler emits:

| Artifact | Naming pattern | Description |
|---|---|---|
| CREATE TABLE (base) | `schema._T` | The actual data table with PK/FK constraints |
| CREATE TABLE _INSERT | `schema._T_INSERT` | Empty clone for insert tracking |
| CREATE TABLE _INSERT_JOIN | `schema._T_INSERT_JOIN` | Empty clone for insert join staging |
| CREATE TABLE _DELETE | `schema._T_DELETE` | Empty clone for delete tracking |
| CREATE TABLE _DELETE_JOIN | `schema._T_DELETE_JOIN` | Empty clone for delete join staging |
| Constraint functions | `schema._T_<type>_<n>_insert_fn` | One per constraint (MVD, FD, INC) on the table |
| INSERT tracking function | `schema._T_INSERT_fn` | Copies NEW row into _INSERT table |
| INSERT tracking trigger | `schema._T_INSERT_trigger` | AFTER INSERT on base table |
| DELETE tracking function | `schema._T_DELETE_fn` | Copies OLD row into _DELETE table |
| DELETE tracking trigger | `schema._T_DELETE_trigger` | AFTER DELETE on base table |
| INSERT JOIN function | `schema._T_INSERT_JOIN_fn` | Builds universal temp table, projects into all _INSERT_JOIN tables |
| INSERT JOIN trigger | `schema._T_INSERT_JOIN_trigger` | AFTER INSERT on _INSERT table |
| DELETE JOIN function | `schema._T_DELETE_JOIN_fn` | Builds universal temp table, projects into all _DELETE_JOIN tables |
| DELETE JOIN trigger | `schema._T_DELETE_JOIN_trigger` | AFTER INSERT on _DELETE table |

Additionally, the following are emitted once per schema (not per table):

| Artifact | Naming pattern | Description |
|---|---|---|
| _LOOP table | `schema._LOOP` | Shared cycle-prevention table |
| Source insert mapping function | `schema.source_insert_fn` | S-to-T final mapping |
| Target insert mapping function | `schema.target_insert_fn` | T-to-S final mapping |
| Source delete mapping function | `schema.source_delete_fn` | S-to-T delete mapping |
| Target delete mapping function | `schema.target_delete_fn` | T-to-S delete mapping |
| Mapping triggers | One per _INSERT_JOIN / _DELETE_JOIN table | All call the same mapping function |
