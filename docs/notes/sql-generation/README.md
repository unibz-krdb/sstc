# SQL Generation

Patterns for the SQL code the compiler must generate. Each file describes a category of generated SQL with the generic template and concrete examples.

- [table-creation.md](table-creation.md) — Base tables, _INSERT tables, _INSERT_JOIN tables (the empty-clone pattern)
- [insert-chain.md](insert-chain.md) — The full INSERT trigger chain: base → tracking → join → mapping
- [delete-chain.md](delete-chain.md) — DELETE propagation and the independence check for partial deletes
- [mapping-functions.md](mapping-functions.md) — The final source_insert_fn / target_insert_fn that map between schemas
