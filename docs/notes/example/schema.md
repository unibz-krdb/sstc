# EMPDEP/POSITION Running Example

This document describes the running example used across the SSTC documentation: a source schema of employee-department data that is vertically decomposed into a normalized target schema via lossless CARM mappings.

---

## Source Schema (S) -- 2 tables

### _EMPDEP

```sql
_EMPDEP(ssn, name, phone, email, dep_name, dep_address)
-- PK(ssn, phone, email)
-- FK(dep_address) -> _POSITION
```

### _POSITION

```sql
_POSITION(dep_address, city, country)
-- PK(dep_address)
```

## Source Constraints

- **MVD:** `ssn ->> phone` (a person can have multiple phones)
- **MVD:** `ssn ->> email` (a person can have multiple emails)
- **FD:** `dep_name -> dep_address`
- **FD:** `city -> country`
- **INC:** `_EMPDEP.dep_address ⊆ _POSITION.dep_address` (foreign key)

## Target Schema (T) -- 6 tables (vertical decomposition / CARM)

### _person

```sql
_person(ssn, name, dep_name)
-- PK(ssn)
-- FK(dep_name) -> _department
```

### _person_phone

```sql
_person_phone(ssn, phone)
-- PK(ssn, phone)
-- FK(ssn) -> _person
```

### _person_email

```sql
_person_email(ssn, email)
-- PK(ssn, email)
-- FK(ssn) -> _person
```

### _department

```sql
_department(dep_name, dep_address)
-- PK(dep_name)
-- FK(dep_address) -> _department_city
```

### _department_city

```sql
_department_city(dep_address, city)
-- PK(dep_address)
-- FK(city) -> _city_country
```

### _city_country

```sql
_city_country(city, country)
-- PK(city)
```

## Target FK Graph

```
_city_country
    ↑
_department_city
    ↑
_department
    ↑
_person ← _person_phone
    ↑
_person_email
```

## Lossless Mappings

From the paper (docs/papers/2407.07502v1.pdf).

### S -> T direction

```
_person          = π_{ssn, name, dep_name}(_EMPDEP)
_person_phone    = π_{ssn, phone}(_EMPDEP)
_person_email    = π_{ssn, email}(_EMPDEP)
_department      = π_{dep_name, dep_address}(_EMPDEP)
_department_city = π_{dep_address, city}(_POSITION)
_city_country    = π_{city, country}(_POSITION)
```

### T -> S direction

```
_EMPDEP   = π_{ssn, name, phone, email, dep_name, dep_address}(_person ⋈ _person_phone ⋈ _person_email ⋈ _department)
_POSITION = π_{dep_address, city, country}(_department_city ⋈ _city_country)
```

## Sample Data

### _POSITION

```sql
INSERT INTO transducer._POSITION VALUES
  ('depadd1', 'Paris', 'France'),
  ('depadd2', 'Roma', 'Italy'),
  ('depadd3', 'London', 'UK');
```

### _EMPDEP

```sql
INSERT INTO transducer._EMPDEP VALUES
  ('ssn1', 'John', 'phone11', 'mail11', 'dep1', 'depadd1'),
  ('ssn1', 'John', 'phone12', 'mail11', 'dep1', 'depadd1'),
  ('ssn2', 'Jane', 'phone21', 'mail21', 'dep2', 'depadd2'),
  ('ssn3', 'June', 'phone31', 'mail31', 'dep3', 'depadd3'),
  ('ssn3', 'June', 'phone31', 'mail32', 'dep3', 'depadd3'),
  ('ssn3', 'June', 'phone32', 'mail31', 'dep3', 'depadd3'),
  ('ssn3', 'June', 'phone32', 'mail32', 'dep3', 'depadd3');
```

Note how ssn3 has 4 tuples due to both MVDs (2 phones × 2 emails).
