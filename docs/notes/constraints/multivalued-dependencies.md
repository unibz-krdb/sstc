# Multivalued Dependencies

A multivalued dependency (MVD) X ->> Y in a relation R(X, Y, Z) states that Y and Z are independent with respect to X. For any four tuples t1, t2, t3, t4 sharing the same X value: t1[Y] = t3[Y], t2[Y] = t4[Y], t1[R\XY] = t4[R\XY], t2[R\XY] = t3[R\XY]. In practice, an X value x1 can have any number of associated Y values as long as the remaining attributes (Z) remain consistent. MVDs require two enforcement mechanisms: a BEFORE INSERT violation check and an AFTER INSERT tuple grounding step.

---

## Composite primary key implications

MVDs change the primary key of the relation. Even if X is the intended semantic key (e.g., a worker ID), the table's primary key must be composite:

- One MVD X ->> Y: primary key becomes XY
- Two MVDs X ->> Y, X ->> Z: primary key becomes XYZ
- n MVDs: primary key grows to include all MVD RHS attributes

For example, a worker with multiple phone numbers produces tuples like:

```
{x1, y1, z1}
{x1, y2, z1}
{x1, y3, z1}
```

## Violation check (BEFORE INSERT trigger)

The basic MVD violation check computes the cross-product of existing Y values with existing Z values for the same X, then checks whether all such combinations exist:

```sql
IF EXISTS (SELECT DISTINCT R1.X, R1.Y, R2.Z
    FROM R1, R2
    WHERE R1.X = R2.X
    EXCEPT
    SELECT * FROM R1) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON R';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

Starting from {x1, y1, z1}, this allows:

- {x2, y2, z2} -- new X value, always valid
- {x1, y2, z1} -- new Y for existing X, Z unchanged, valid

But rejects:

- {x1, y3, z3} -- new Y with new Z, violates independence even though it does not violate the composite key XY

## Multiple MVDs with shared LHS

When multiple MVDs share the same left-hand side, the individual single-MVD check queries do not work. Given R(X, Y, Z, T) with X ->> Y and X ->> Z, these two independent checks fail:

```sql
-- Does NOT work for X ->> Y
SELECT DISTINCT R1.X, R1.Y, R2.Z, R2.T
FROM R1, R2
WHERE R1.X = R2.X
EXCEPT
SELECT * FROM R1

-- Does NOT work for X ->> Z
SELECT DISTINCT R1.X, R2.Y, R1.Z, R2.T
FROM R1, R2
WHERE R1.X = R2.X
EXCEPT
SELECT * FROM R1
```

The problem: {x1, y2, z1, t1} passes the first check but fails the second, and {x1, y1, z2, t1} does the reverse. Both tuples should be valid.

The solution for shared-LHS MVDs keeps all Yi attributes from R1 and only takes the remaining attributes (Z/T) from R2:

```sql
IF EXISTS (SELECT DISTINCT R1.X, R1.Y, R1.Z, R2.T
    FROM R1, R2
    WHERE R1.X = R2.X
    EXCEPT
    SELECT * FROM R1) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON R';
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

This allows any variation of individual Y and Z values as long as X and T are fixed. For example, {x1, y2, z2, t1} and {x1, y3, z1, t1} are accepted, while {x1, y2, z3, t2} is rejected.

## Non-shared-LHS MVDs

MVDs with different left-hand sides (e.g., X ->> Z and XY ->> T) do not work with this shared-LHS approach. This remains an open problem. Decomposition of the relation may be necessary to handle such cases.

## Tuple grounding (AFTER INSERT trigger)

The violation check alone is insufficient. MVDs require completeness: when a new Y value is introduced for an existing X, all cross-product combinations with existing Z values must also exist. This follows from the MVD definition -- for any pair XY, there must exist corresponding XZT tuples for all known Z values.

This is equivalent to what would result from decomposing R into projections (e.g., RXT, RXY, RXZ) and computing their full join.

The grounding function runs AFTER INSERT and auto-generates the missing tuples:

```sql
CREATE OR REPLACE FUNCTION CHECK_R1_A_MVD_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
  IF EXISTS (SELECT R1.X, R1.Y, R2.Z, R1.T
      FROM R1, R2
      WHERE R1.X = R2.X
      UNION
      SELECT R1.X, R2.Y, R1.Z, R1.T
      FROM R1, R2
      WHERE R1.X = R2.X
      EXCEPT
      (SELECT * FROM R1)) THEN
    RAISE NOTICE 'THE TUPLE % LEAD TO ADDITIONAL ONES', NEW;
    INSERT INTO R
        (/*same query as above*/);
    RETURN NEW;
  ELSE
    RETURN NEW;
  END IF;
END;
$$;
```

### Example with 2 MVDs (phone, email)

For a relation with SSN, name, phone, email, dep_name, dep_address where X ->> phone and X ->> email:

```sql
SELECT r1.ssn, r1.name, r1.phone, NEW.email, r1.dep_name, r1.dep_address
FROM R AS r1 WHERE r1.ssn = NEW.ssn
UNION
SELECT r1.ssn, r1.name, NEW.phone, r1.email, r1.dep_name, r1.dep_address
FROM R AS r1 WHERE r1.ssn = NEW.ssn
EXCEPT
(SELECT * FROM R)
```

Each UNION branch swaps one MVD attribute (Yi) between R1 and NEW, generating the missing cross-product tuples. The EXCEPT removes any tuples that already exist.

## Scaling pattern for n MVDs with shared LHS

For n MVDs of the form X ->> Y1, X ->> Y2, ..., X ->> Yn in R(X, Y1, Y2, ..., Yn, Z):

**Violation check:**

```sql
SELECT DISTINCT R1.X, R1.Y1, R1.Y2, ..., R1.Yn, R2.Z
FROM R1, R2
WHERE R1.X = R2.X
EXCEPT
SELECT * FROM R1
```

**Tuple grounding** (UNION of n queries, each swapping one Yi):

```sql
SELECT R1.X, R2.Y1, R1.Y2, ..., R1.Yn, R1.Z
FROM R1, R2
WHERE R1.X = R2.X
UNION
SELECT R1.X, R1.Y1, R2.Y2, ..., R1.Yn, R1.Z
FROM R1, R2
WHERE R1.X = R2.X
UNION
...
UNION
SELECT R1.X, R1.Y1, R1.Y2, ..., R2.Yn, R1.Z
FROM R1, R2
WHERE R1.X = R2.X
EXCEPT
(SELECT * FROM R1)
```

The growing complexity of these constraints makes decomposition increasingly necessary.
