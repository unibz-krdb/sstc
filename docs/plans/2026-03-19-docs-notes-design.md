# Design: docs/notes structured documentation

## Goal

Create a `docs/notes/` directory containing compiled, organized reference material extracted from the raw `notes/` SQL files. The primary consumer is Claude (LLM context), secondarily the developer. Files should be small, focused, and self-contained so they can be loaded surgically without wasting context window.

## Directory structure

```
docs/
├── papers/
│   ├── README.md
│   └── 2407.07502v1.pdf          (moved from notes/)
└── notes/
    ├── README.md
    ├── architecture/
    │   ├── README.md
    │   ├── layers.md
    │   ├── loop-prevention.md
    │   └── timing-and-ordering.md
    ├── constraints/
    │   ├── README.md
    │   ├── functional-dependencies.md
    │   ├── multivalued-dependencies.md
    │   ├── guard-dependencies.md
    │   └── conditional-join-dependencies.md
    ├── sql-generation/
    │   ├── README.md
    │   ├── table-creation.md
    │   ├── insert-chain.md
    │   ├── delete-chain.md
    │   └── mapping-functions.md
    ├── example/
    │   ├── README.md
    │   ├── schema.md
    │   └── reference-output.sql
    └── open-problems.md
```

19 files total. Each content file targets 100-300 lines.

## File format convention

Every markdown file:

```markdown
# Title

One-paragraph summary of what this file covers and when to consult it.

---

<content with SQL code blocks>
```

README indexes:

```markdown
# <Directory Name>

One-line description of this section.

- [file-name.md](file-name.md) — what it covers
```

No frontmatter, no metadata.

## Content mapping

| Source file | Destination |
|---|---|
| transducer_definition.sql (layers) | architecture/layers.md |
| transducer_definition.sql (loop) | architecture/loop-prevention.md |
| transducer_definition.sql (timing/ordering) | architecture/timing-and-ordering.md |
| constraint_definition.sql (FD) | constraints/functional-dependencies.md |
| constraint_definition.sql (MVD) | constraints/multivalued-dependencies.md |
| constraint_definition.sql (guard) | constraints/guard-dependencies.md |
| constraint_definition.sql (CJD) | constraints/conditional-join-dependencies.md |
| desired_output.sql (structure) | sql-generation/table-creation.md |
| output.sql (INSERT chain) | sql-generation/insert-chain.md |
| updates_and_more.sql + output.sql | sql-generation/delete-chain.md |
| output.sql (mapping functions) | sql-generation/mapping-functions.md |
| All files (example schema) | example/schema.md |
| desired_output.sql (cleaned) | example/reference-output.sql |
| Scattered open issues | open-problems.md |
| 2407.07502v1.pdf | docs/papers/2407.07502v1.pdf |

## What's not included

- Paper content is not reproduced — referenced by path only
- Inclusion dependencies: insufficient detail in notes, listed in open-problems.md
- DELETE tracking tables: symmetric to INSERT, noted in insert-chain.md rather than duplicated
- Raw notes/ files remain as historical artifacts

## Design decisions

- **Small files over large ones**: Claude can load exactly what it needs without truncation or context waste
- **Predictable naming**: Claude can guess the right file from a topic keyword
- **README indexes per directory**: Claude reads the index to navigate
- **Leading summary paragraph**: Claude can confirm it found the right document before reading further
- **SQL code blocks throughout**: the docs are about SQL generation, so concrete examples are essential
- **Paper stays as PDF in docs/papers/**: no point reproducing academic content, just reference it
