# Semantic SQL Transducer Prototype

This is a Python-based prototype for the [Semantic SQL Transducer](https://github.com/unibz-krdb/SemanticSQLTransducer).

## Requirements

- [Python3.11](https://www.python.org/) or greater 
- [PostgreSQL](https://www.postgresql.org/)

## Usage

*Setup*

```shell
python3 -m venv venv
./venv/bin/activate
pip3 install .
```

*Testing*

```shell
pytest
```

## Explanation

### Input

The Semantic SQL Transducer takes *sql* files as input. Three sets of SQL input files for the source, target and universal databases are required. 
There is a strict naming convention for these SQL files. This is so metadata can be stored within the name itself, removing the need for an additional metadata file.

*NB: Table names must fully qualified, i.e., {schema}.{tablename}, not just {tablename}*

**1. Create**

For the *source* database, there are one-to-many *create* files containing a create statement describing the table, as well as any key alterations.

e.g., 
``` sql
CREATE TABLE transducer._PERSON
    (
      ssn VARCHAR(100) NOT NULL,
      phone VARCHAR(100) NOT NULL,
      manager VARCHAR(100),
      title VARCHAR(100),
      city VARCHAR(100) NOT NULL,
      country VARCHAR(100) NOT NULL,
      mayor VARCHAR(100) NOT NULL
    );

ALTER TABLE transducer._person ADD PRIMARY KEY (ssn,phone);
```

For the *target* database, there are one-to-many *create* files, each corresponding to a table in the *target* database. These each contain a create statement which references table(s) from the *source* database.

e.g., 
``` sql
CREATE TABLE transducer._CITY AS
SELECT DISTINCT city, country,mayor FROM transducer._person;
ALTER TABLE transducer._city ADD PRIMARY KEY (city);
ALTER TABLE transducer._city
ADD FOREIGN KEY (mayor) REFERENCES transducer._person_ssn(ssn);
```

```
%SCHEMA%.%TABLENAME%.sql
```

The *universal* folder contains a single file named `attributes.sql`. This contains the attributes for a universal table, e.g.,

``` sql
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
```

**2. Constraints**

Constraints, both for the *source* schema and *target* schema, are functions.

e.g.,
``` sql
CREATE OR REPLACE FUNCTION transducer._person_inc_2_delete_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

   /*In two time, we first check if the deleted tuple actually remove VALUES in ssn or mayor */
   IF NOT EXISTS (SELECT * FROM transducer._person WHERE ssn = OLD.ssn
   EXCEPT (SELECT * FROM transducer._person WHERE ssn = OLD.ssn AND phone = OLD.phone)) THEN
      /*If so, then we check is other tuple are dependent on those values*/
      IF EXISTS (SELECT ssn, phone
        FROM transducer._person WHERE mayor = OLD.ssn
        EXCEPT(SELECT OLD.ssn, OLD.phone)) THEN
         /*If so, then it violates the inclusion dependency constraint*/
            RAISE EXCEPTION 'THIS REMOVED VALUES VIOLATE THE INC2 CONSTRAINT';
            RETURN NULL;
      END IF;
   END IF;
   RETURN OLD;
END;
$$;
```

```
%SCHEMA%.%TABLENAME%.%TYPE%.%ID%.%INSERT/DELETE%.sql
```

- *TYPE*: Type of the constraint, e.g., cfd
- *ID*: ID number to distinguish this from other constraints of the same type which trigger at the same time
- *INSERT/DELETE*: when this constraint should trigger

**3. Mappings**

The *source* schema has one *mapping* file per *source* table, mapping *target* table(s) to the *source* table.

``` sql
SELECT *
FROM _person_ssn
NATURAL JOIN _person_phone
NATURAL JOIN _city
NATURAL JOIN
    (
      SELECT *
      FROM _person_manager
      NATURAL JOIN _manager
      UNION
      SELECT ssn, null, null, city
      FROM _person_no_manager
    );
```

Like with *create* files, the *target* schema has one *mapping* file per *target* table. These often contain select statements referencing the *source* table.

e.g., 
``` sql
SELECT $S0.ssn, $S0.manager, $S0.city
FROM $S0._person
WHERE $S0.manager IS NOT NULL AND $S0.title IS NOT NULL
```

As one can see from the example above, the schema-table name is replaced by a `$S0` placeholder. During execution, these placeholders are replaced by an appropriate prefix.

```
%SCHEMA%.%TARGET%.%SOURCE1%.%SOURCE2%.%SOURCEN%.sql
```

- *TARGET*: Target tablename
- *SOURCE1*, *SOURCE2*, ..., *SOURCEN*: Source tables involved in the mapping

The *universal* mappings function slightly different. There are two sets: *from* and *to*. The *from* mappings map from the universal table to the target/source tables. The *to* mappings map from the target/source tables to the universal table.

*from* example:

``` sql
-- _empdep.sql
SELECT ssn, name, phone, email, dep_name, dep_address FROM ${universal_tablename}
```

*to* example:

``` sql
-- _empdep._position.sql
SELECT ssn, name, phone, email, dep_name, dep_address FROM ${universal_tablename}
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._POSITION${primary_suffix}
NATURAL LEFT OUTER JOIN transducer._EMPDEP
```

#### Structure

In order to distinguish which input files are which, a directory containing the files is passed as input to the program. This directory has the following structure:

```
input
L constraints
|   L source
|   L target
L create
|   L source
|   L target
|   L universal
L mappings
    L source
    L target
    L universal
        L from
        L to
```

### Example

Check `test/resources/input` for an example input directory.
Running `pytest` will execute the Semantic SQL Transudcer on this directory, resulting in `output.sql` being produced in the project directory.

## Contribution

This project is still too young to accept outside contribution, but soon!
