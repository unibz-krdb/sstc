# Architecture

The Semantic SQL Transducer's three-layer trigger-based architecture for bidirectional schema synchronization.

- [layers.md](layers.md) — Base tables, update tracking layer, and join layer
- [loop-prevention.md](loop-prevention.md) — The _LOOP table mechanism that prevents infinite trigger recursion
- [timing-and-ordering.md](timing-and-ordering.md) — INSERT/DELETE ordering for foreign keys, NATURAL JOIN ordering, and the wait mechanism
