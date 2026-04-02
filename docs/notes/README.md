# Notes

Compiled reference material for the Semantic SQL Transducer Compiler. Extracted from raw `notes/` SQL files and organized for quick lookup.

See also: [papers/](../papers/) for the academic paper.

## Sections

- [architecture/](architecture/) — The three-layer transducer architecture, loop prevention, timing
- [constraints/](constraints/) — SQL implementations of FDs, MVDs, guard deps, conditional join deps
- [sql-generation/](sql-generation/) — Patterns for generating tables, trigger chains, and mapping functions
- [open-problems.md](open-problems.md) — Unsolved issues and areas needing further work

- [example/](example/) — The PERSON URA running example (NULLs, CFDs, horizontal decomposition). Authoritative reference SQL for the transducer architecture, referenced throughout these docs
