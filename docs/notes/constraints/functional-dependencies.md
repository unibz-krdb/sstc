# Functional Dependencies

A functional dependency (FD) in a relation R(X, Y, Z) with primary key X is written Y -> Z. It states that for any two tuples t1, t2 in R, if t1[Y] = t2[Y] then t1[Z] = t2[Z]. FDs are enforced at INSERT time using BEFORE INSERT trigger functions that check whether the incoming tuple would violate the dependency, raising an exception if so and returning NULL as the fail state.

---

## Generic FD check pattern

Given R(X, Y, Z) with FD Y -> Z, the check query cross-references the NEW tuple against all existing tuples, matching on the LHS and looking for disagreement on the RHS:

```sql
IF EXISTS (SELECT * FROM R AS R1,
    (SELECT NEW.X, NEW.Y, NEW.Z) AS R2
    WHERE R1.Y = R2.Y
    AND R1.Z <> R2.Z) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN R';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

The generic form replaces concrete attribute names with LHS/RHS placeholders:

```sql
SELECT * FROM R1, R2
    WHERE R1.FD[LHS] = R2.FD[LHS]
    AND R1.FD[RHS] <> R2.FD[RHS]
```

## Materializing NEW as a subselect

In PostgreSQL trigger functions, the NEW record (the tuple being inserted) cannot be referenced directly as a table in the FROM clause. It must be materialized as a subselect: `(SELECT NEW.X, NEW.Y, NEW.Z) AS R2`. Throughout the remaining examples, R2 is used as shorthand for this materialized NEW tuple.

## BEFORE INSERT trigger template

Each FD check function is attached to the table via a BEFORE INSERT trigger:

```sql
CREATE OR REPLACE TRIGGER R_FD_1
BEFORE INSERT ON R
FOR EACH ROW
EXECUTE FUNCTION CHECK_R_FD_1_FN();
```

The trigger function structure:

```sql
CREATE OR REPLACE FUNCTION CHECK_R_FD_1_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF (/*FD TEST*/) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN R';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;
```

## Overlapping FDs

When multiple FDs exist on the same relation, each FD gets its own independent trigger function. No special handling for overlap is needed -- the triggers fire independently and each checks only its own dependency. Below are three cases on R(X, Y, Z, T) starting with tuples {x1, y1, z1, t1} and {x2, y2, z2, t2}.

### Case 1: Y -> T, Z -> T (two independent FDs, same RHS)

Two separate check functions, one per FD:

```sql
-- FD1: Y -> T
IF EXISTS (SELECT * FROM R1, R2
    WHERE R1.Y = R2.Y
    AND R1.T <> R2.T) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD1 CONSTRAINT';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

```sql
-- FD2: Z -> T
IF EXISTS (SELECT * FROM R1, R2
    WHERE R1.Z = R2.Z
    AND R1.T <> R2.T) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD2 CONSTRAINT';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

Allows tuples like {x3, y2, z2, t2} or {x4, y1, z1, t1}. Prevents tuples like {x5, y1, z1, t2} (violates FD1) or {x6, y2, z1, t1} (violates FD2).

### Case 2: Y -> Z, Z -> T (transitive FDs)

```sql
-- FD1: Y -> Z
IF EXISTS (SELECT * FROM R1, R2
    WHERE R1.Y = R2.Y
    AND R1.Z <> R2.Z) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD1 CONSTRAINT';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

```sql
-- FD2: Z -> T
IF EXISTS (SELECT * FROM R1, R2
    WHERE R1.Z = R2.Z
    AND R1.T <> R2.T) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD2 CONSTRAINT';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

With an additional tuple {x3, y3, z3, t1}, allows tuples like {x4, y3, z3, t1} where both FDs are satisfied through the transitive chain.

### Case 3: YZ -> T, T -> Z (interdependent FDs)

```sql
-- FD1: YZ -> T (composite LHS)
IF EXISTS (SELECT * FROM R1, R2
    WHERE R1.Y = R2.Y
    AND R1.Z = R2.Z
    AND R1.T <> R2.T) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD1 CONSTRAINT';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

```sql
-- FD2: T -> Z
IF EXISTS (SELECT * FROM R1, R2
    WHERE R1.T = R2.T
    AND R1.Z <> R2.Z) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD2 CONSTRAINT';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

Despite the circular nature of this constraint (YZ determines T, T determines Z), the independent trigger functions correctly prevent any tuple where YZT are interdependent. Only tuples like {x3, y1, z1, t1} that satisfy both constraints simultaneously are allowed.

## Key takeaway

Each FD translates to one independent trigger function. Overlapping, transitive, and interdependent FDs all work correctly with this approach because each trigger checks only its own LHS/RHS constraint -- no special-case handling or cross-dependency logic is required.
