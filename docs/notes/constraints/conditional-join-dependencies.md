# Conditional Join Dependencies

A conditional join dependency (CJD) is a functional dependency that only holds when the covered attributes are non-null. This models real-world scenarios where entities specialize into subtypes -- a person may or may not be an employee, and employee attributes only constrain each other when they are present. The enforcement combines an FD violation check with a joint-null guard in a single trigger function.

---

## Basic example

Given Person(ssn, name, dep_name, dep_address) where dep_name and dep_address are nullable and dep_name -> dep_address holds conditionally (only when both are non-null). Valid tuples include:

```
{'ssn1', 'John', 'dep1', 'depadd1'}   -- FD applies: dep1 -> depadd1
{'ssn2', 'Jane', 'dep2', 'depadd2'}   -- FD applies: dep2 -> depadd2
{'ssn3', 'Jovial', NULL, NULL}         -- FD does not apply: both null
```

The FD still rejects tuples like {'ssn4', 'June', 'dep1', 'depadd2'} because dep_name is non-null and dep1 already maps to depadd1.

## Combined check query

The CJD check merges three conditions into a single IF EXISTS block. Using the generic form R(X, Y, Z, T) with CJD Z -> T (when Z and T are non-null):

```sql
IF EXISTS (SELECT *
    FROM R1, R2
    WHERE (R2.Z IS NOT NULL AND R2.T IS NOT NULL
           AND R1.Z = R2.Z
           AND R1.T <> R2.T)
       OR (R2.Z IS NULL AND R2.T IS NOT NULL)
       OR (R2.Z IS NOT NULL AND R2.T IS NULL)) THEN
  RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT %', NEW;
  RETURN NULL;
ELSE
  RETURN NEW;
END IF;
```

The three OR branches handle:

1. **FD violation**: both Z and T are non-null, Z values match but T values disagree
2. **Invalid null pattern**: Z is NULL but T is not (partial null, not jointly null)
3. **Invalid null pattern**: T is NULL but Z is not (partial null, not jointly null)

This effectively combines the FD check with the joint-null guard -- allowing tuples where both Z and T are NULL to bypass the FD check entirely, while preventing partial-null tuples that would be problematic given the FD.

## Overlapping CJDs (specialization hierarchies)

CJDs naturally model specialization hierarchies. Consider R(ssn, name, empid, hdate, dep_name) with two conditional FDs:

- fd[R](empid, hdate) when empid and hdate are non-null
- fd[R](empid, dep_name) when empid, hdate, and dep_name are all non-null

This represents a three-level hierarchy:

1. **Person**: only ssn and name are required
2. **Employee**: person specialized with empid and hdate
3. **Department employee**: employee further specialized with dep_name

Valid tuples:

```
{'ssn1', 'John', 'id1', 'date1', 'dep1'}   -- full department employee
{'ssn2', 'June', 'id2', 'date2', NULL}      -- employee without department
{'ssn3', 'Joel', NULL, NULL, NULL}           -- person only
```

The first CJD (empid -> hdate) only applies when both empid and hdate are non-null, allowing persons without employee data. The second CJD (empid -> dep_name) only applies when empid, hdate, and dep_name are all non-null, allowing employees without department assignments. Each CJD gets its own trigger function following the combined check pattern above, with the null-pattern branches adjusted to match the relevant attribute group.
