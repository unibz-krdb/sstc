# Guard Dependencies

A guard dependency g(cond) R != NULL denotes the potential presence of tuples satisfying a condition in a relation R. While guards on concrete values (e.g., g(Z = '3') R != NULL) are trivial, guards expressing jointly-null attributes -- where a group of attributes must be NULL together or not at all -- cannot be expressed natively in SQL and require trigger-based enforcement. Overlapping joint-null constraints further complicate matters, as naive independent checks fail and must be replaced by an enumeration of all valid null-patterns.

---

## Jointly-null attributes (simple case)

Given R(X, Y, Z, T) where Z and T must be NULL together (non-overlapping), the check rejects any tuple where exactly one of the two is NULL:

```sql
IF EXISTS (SELECT * FROM R
    WHERE (NEW.Z IS NOT NULL AND NEW.T IS NULL)
    OR    (NEW.Z IS NULL AND NEW.T IS NOT NULL)) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

This is a BEFORE INSERT trigger that rejects tuples where Z and T have mismatched nullability.

## Overlapping guards (naive approach fails)

When joint-null constraints overlap, independent check functions do not work. Consider R(X, Y, Z, T) with two constraints: YZ jointly nullable and ZT jointly nullable. The valid tuple patterns are:

```
{x, y, z, t}           -- all non-null
{x, NULL, NULL, t}      -- YZ null together, T non-null
{x, y, NULL, NULL}      -- ZT null together, Y non-null
{x, NULL, NULL, NULL}   -- both groups null
```

Two independent trigger functions checking each pair separately:

```sql
-- Check YZ jointly null
CREATE OR REPLACE FUNCTION transducer.CHECK_R_GJ_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
  IF EXISTS (SELECT * FROM transducer.R
      WHERE (NEW.Y IS NOT NULL AND NEW.Z IS NULL)
      OR    (NEW.Y IS NULL AND NEW.Z IS NOT NULL)) THEN
    RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
    RETURN NULL;
  ELSE
    RETURN NEW;
  END IF;
END;
$$;

-- Check ZT jointly null
CREATE OR REPLACE FUNCTION transducer.CHECK_R_GJ_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
  IF EXISTS (SELECT * FROM transducer.R
      WHERE (NEW.Z IS NOT NULL AND NEW.T IS NULL)
      OR    (NEW.Z IS NULL AND NEW.T IS NOT NULL)) THEN
    RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
    RETURN NULL;
  ELSE
    RETURN NEW;
  END IF;
END;
$$;
```

This fails because valid tuples like {x, NULL, NULL, t} (Y and Z null, T non-null) violate the ZT check, and {x, y, NULL, NULL} (Z and T null, Y non-null) violates the YZ check. Relaxing to allow all three attributes to be independently nullable would permit invalid tuples like {x, y, NULL, t}.

## Working solution: enumerate valid null-patterns

Instead of checking each constraint independently, enumerate all valid null-patterns in a single function using NOT EXISTS. A valid disposition is one that satisfies all joint-null constraints simultaneously:

```sql
CREATE OR REPLACE FUNCTION transducer.CHECK_R_GJ_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
  IF NOT EXISTS (SELECT NEW.*
      WHERE (NEW.X IS NOT NULL AND NEW.Y IS NOT NULL AND NEW.Z IS NOT NULL AND NEW.T IS NOT NULL)
      OR    (NEW.X IS NOT NULL AND NEW.Y IS NULL AND NEW.Z IS NULL AND NEW.T IS NOT NULL)
      OR    (NEW.X IS NOT NULL AND NEW.Y IS NOT NULL AND NEW.Z IS NULL AND NEW.T IS NULL)
      OR    (NEW.X IS NOT NULL AND NEW.Y IS NULL AND NEW.Z IS NULL AND NEW.T IS NULL)) THEN
    RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
    RETURN NULL;
  ELSE
    RETURN NEW;
  END IF;
END;
$$;
```

Each OR branch corresponds to one valid null-pattern. The NEW tuple must match at least one pattern to be accepted. If it matches none, the constraint is violated.

## Automatic disposition grounding

The valid null-patterns can be computed automatically from a set of guard constraints using an algorithm that enumerates all satisfying assignments. This avoids manually listing the valid dispositions, which becomes impractical as the number of overlapping guards grows. In practice, however, extensive joint-null overlap is uncommon.
