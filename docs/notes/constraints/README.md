# Constraints

SQL trigger functions that enforce relational constraints not natively supported by PostgreSQL. Each constraint type has a BEFORE INSERT check function and, where needed, an AFTER INSERT grounding function.

- [functional-dependencies.md](functional-dependencies.md) — FD violation checks, overlapping FDs
- [multivalued-dependencies.md](multivalued-dependencies.md) — MVD violation checks and automatic tuple grounding
- [guard-dependencies.md](guard-dependencies.md) — Jointly-null attribute constraints
- [conditional-join-dependencies.md](conditional-join-dependencies.md) — FDs that only apply when covered attributes are non-null
