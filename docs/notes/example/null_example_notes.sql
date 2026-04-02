/*
In this example I tried to write a more complex example for the transducer architecture based on the motivating example we use in our paper.
It's a URA database, so a database with a single table, containing:

Person:(ssn, empid, name, hdate, phone, email, dept, manager)

Within this table, several constraints holds:
- MVD ssn ->> phone
- MVD ssn ->> email
- IND manager C ssn
- CFD empid -> hdate (empid, hdate != NULL)
- CFD empid -> dept (empid, hdate, dept, manager != NULL)
- CFD dept -> manager (empid, hdate, dept, manager != NULL)

Informally, what this table contains is several persons, some of which may be employee, and thus have a non-null empid, 
and some of these employee can be free or assigned to a specific department.
There is an obvious hierarchy of concepts existing in this table defining: empd_dep C emp C person, 
plus one more if you count the manager also always being a subset of emp_dep: manager C empdep.
The difficulty of this example naturally comes from this semantically hidden hierarchy which in has to be expressed only
in database constraints, some of which overlaps. 


The decomposition I propose can be argued, it's not in 6NF, but I think that it's a decent decomposition of this table nonetheless.
It goes like this, depth intended:

PERSON:		P:(ssn, name)		PERSON_PHONE:(ssn, phone)		PERSON_EMAIL:(ssn, EMAIL)
EMPLOYEE	PE:(ssn, empid)		PE_HDATE:(empid, hdate)
EMPDEP:		PED:(ssn, empid)	PED_DEPT:(empid, dept)			DEPT_MANAGER:(dept, manager)

This example notably illustrate how the transducer architecture work when null value are included.
And honestly, beside a few bumps here and here, it works pretty well.
So let's get started:

Starting with the definition of the conditional functional dependencies:
*/

CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
		   FROM transducer._PERSON AS R1, 
		   (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
		   WHERE (R2.empid IS NOT NULL AND R2.dept IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL 
				AND R1.empid = R2.empid 
				AND R1.dept <> R2.dept) 
				OR (R2.empid IS NULL AND R2.dept IS NOT NULL)
				OR (R2.empid IS NULL AND R2.manager IS NOT NULL)
				OR (R2.empid IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NULL)
				OR (R2.empid IS NOT NULL AND R2.dept IS NULL AND R2.manager IS NOT NULL)) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> dept %', NEW;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;
END;
$$;

/*
What's to note here is the long list of conditions written here. This particular CFD correspond to CFD empid -> dept (empid, hdate, dept, manager != NULL),
so to hold it require all four of these attributes to be non-null. Easy. Except that's not how we write it in SQL. Instead we try to catch every possible wrong INSERTs.
Which would require, for instance to check that if dept is non-null, manager can't be null. And the reverse, if manager is non-null then dept can't be null.
And also check that if dept is non-null then empid can't be null. And so on.

I opted for a shortened version, but for the code generation it might be necessary to check all options.


Another thing to note is that I had to play around the foreign key a bit to get something working. This notably applies to the following
*/

ALTER TABLE transducer._PED_DEPT
ADD FOREIGN KEY (empid) REFERENCES transducer._PED(empid);

ALTER TABLE transducer._PED_DEPT
ADD FOREIGN KEY (dept) REFERENCES transducer._DEPT_MANAGER(dept);

ALTER TABLE transducer._DEPT_MANAGER
ADD FOREIGN KEY (manager) REFERENCES transducer._P(ssn);

/*
What happen here is that I had to make the table PED_DEPT a foreign key of DEPT_MANAGER, instead of the inverse, a more intuitive construction.
I'm not sure why I had to, but I had to. Seems annoying to generate anyway. It has further repercussion on the INSERT ordering later on but that's it.


Moving on to the functions, most of them follow the given template, the only exception being the source and target INSERT function, as expected.
Let's look at the INSERT from PERSON, from the source:
*/

CREATE OR REPLACE FUNCTION transducer.SOURCE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Something got added in a JOIN table';
IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on TARGET';

   INSERT INTO transducer._P (SELECT DISTINCT ssn, name FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND name IS NOT NULL) ON CONFLICT (ssn) DO NOTHING;


   /* Okay, regarding the INSERTs into subtables, I'm not sure how to generalize this. For instance, we have a table _P which contains every persons of _PERSON and another
      two tables _PE and _PED, both defining persons with a non-null empid attribute and a non-null dept attribute for the second.
      Checking for these is easy in our current URA scenario, but in a many to many table case, I think we would recreate the join table and check it */
   
   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
      INSERT INTO transducer._PE (SELECT DISTINCT ssn, empid FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
      INSERT INTO transducer._PE_HDATE (SELECT DISTINCT empid, hdate FROM transducer._PERSON_INSERT_JOIN 
                              WHERE empid IS NOT NULL AND hdate IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._PED (SELECT DISTINCT ssn, empid FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._DEPT_MANAGER (SELECT DISTINCT dept, manager FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (dept) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._PED_DEPT (SELECT DISTINCT empid, dept FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT DO NOTHING;
   END IF;



   INSERT INTO transducer._PERSON_PHONE (SELECT DISTINCT ssn, phone FROM transducer._PERSON_INSERT_JOIN
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,phone) DO NOTHING;

   INSERT INTO transducer._PERSON_EMAIL (SELECT DISTINCT ssn, email FROM transducer._PERSON_INSERT_JOIN
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,email) DO NOTHING;


   DELETE FROM transducer._PERSON_INSERT;
   DELETE FROM transducer._PERSON_INSERT_JOIN;
   DELETE FROM transducer._loop;
   RETURN NEW;
END IF;
END;  $$;

/*

First thing to note, a lot of conditions are required. It makes sense as if a employee is not INSERTed, then it's impossible to add its hdate to the corresponding table.
It seems like a nightmare to generate however as it require to know which conditions holds in the decomposed database. If you know that empid,hdate alone must be non-null
in PE, then there should be a way to translate that information into the proper condition. Still, it's easier said than done.
Second thing to note, turns out it's possible to simplify the ON CONFLICT clause to not focus on a specific value and just be generic. Perhaps this could be generalized?


It actually get a bit worse for the target INSERT:
*/

CREATE OR REPLACE FUNCTION transducer.target_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

RAISE NOTICE 'Function transducer.target_insert_fn called';

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _PERSON';
        
create temporary table temp_table_join (
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table_join (
   SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN transducer._PE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PED_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
   WHERE ssn IS NOT NULL AND NAME IS NOT NULL 
   AND phone IS NOT NULL AND email IS NOT NULL
   AND ((empid IS NULL AND hdate IS NULL)
      OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL)
      OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL)));

IF EXISTS (SELECT * FROM temp_table_join 
         EXCEPT (SELECT * FROM temp_table_join WHERE empid IS NULL)) THEN
   RAISE NOTICE 'Annoying scenario in which the temp join table contains multiple correct tuples';
   IF EXISTS (SELECT * FROM temp_table_join 
         EXCEPT (SELECT * FROM temp_table_join WHERE dept IS NULL)) THEN
      DELETE FROM temp_table_join WHERE dept IS NULL;
   ELSE
      DELETE FROM temp_table_join WHERE empid IS NULL;
   END IF;     
END IF;

INSERT INTO transducer._PERSON (SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table_join) ON CONFLICT (ssn, phone, email) DO NOTHING;


DELETE FROM transducer._P_INSERT;
DELETE FROM transducer._PE_INSERT;
DELETE FROM transducer._PE_HDATE_INSERT;
DELETE FROM transducer._PED_INSERT;
DELETE FROM transducer._PED_DEPT_INSERT;
DELETE FROM transducer._DEPT_MANAGER_INSERT;
DELETE FROM transducer._PERSON_PHONE_INSERT;
DELETE FROM transducer._PERSON_EMAIL_INSERT;

DELETE FROM transducer._P_INSERT_JOIN;
DELETE FROM transducer._PE_INSERT_JOIN;
DELETE FROM transducer._PE_HDATE_INSERT_JOIN;
DELETE FROM transducer._PED_INSERT_JOIN;
DELETE FROM transducer._PED_DEPT_INSERT_JOIN;
DELETE FROM transducer._DEPT_MANAGER_INSERT_JOIN;
DELETE FROM transducer._PERSON_PHONE_INSERT_JOIN;
DELETE FROM transducer._PERSON_EMAIL_INSERT_JOIN;



DELETE FROM transducer._loop;
DELETE FROM temp_table_join;
DROP TABLE temp_table_join;

RETURN NEW;
END IF;
END;    $$;

/*
Obviously, joining back all INSERT_JOIN tables get a bit more difficult once NULL values are thrown in as it is no longer possible to ignore plenty
of redundant incorrect row like we did before by requiring all attributes to be non-null. Here, a post join process is required to keep only the most
correct tuples.
This is done in part with the extended query:
*/

SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN transducer._PE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PED_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
   WHERE ssn IS NOT NULL AND NAME IS NOT NULL 
   AND phone IS NOT NULL AND email IS NOT NULL
   AND ((empid IS NULL AND hdate IS NULL)
      OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL)
      OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL))

/*
What this query does is prevent impossible tuples to be added. Tuples such as ones containing a dept non-null but a manager null, stuff like that.
However, what this query doesn't prevent is for this temporary table to hold multiple distinct, yet correct rows.
To illustrate, let's say we INSERT in a transaction the employee {'ssn5', 'emp5', 'Jex', 'hdate5', 'phone51', 'mail51', NULL, NULL}.
In the process, a lot of null values and redundancy gets added to the join tables. So much so the temporary table of their join always contains the following two rows:
{{'ssn5', 'emp5', 'Jex', 'hdate5', 'phone51', 'mail51', NULL, NULL},
{'ssn5', NULL, 'Jex', NULL, 'phone51', 'mail51', NULL, NULL}}.
This is really tricky as both tuples are correct, but the first one contains more information and is thus better to INSERT into PERSON.
However, left as it is, both INSERT occurs and a correct violation of PK happens.
My temporary answer to that is this following block:
*/

IF EXISTS (SELECT * FROM temp_table_join 
         EXCEPT (SELECT * FROM temp_table_join WHERE empid IS NULL)) THEN
   RAISE NOTICE 'Annoying scenario in which the temp join table contains multiple correct tuples';
   IF EXISTS (SELECT * FROM temp_table_join 
         EXCEPT (SELECT * FROM temp_table_join WHERE dept IS NULL)) THEN
      DELETE FROM temp_table_join WHERE dept IS NULL;
   ELSE
      DELETE FROM temp_table_join WHERE empid IS NULL;
   END IF;     
END IF;

/*
What this section accomplish is to basically fold the temporary table into itself, leaving only the tuples richest in information.
It's a real simple way of checking for tuple containment, a noting in relational database theory stating, in brief, that in two given tuples of the same
signature with equal values, the one containing the more null values is contained within the other. In our example, we want to check for containment in two main cases:
- One in which there is no null values in one row, and some in the other
- Another in which empid and hdate are non-null, but dept and manager are in comparison to a tuple where empid is null.
I'm sure there is a clever query that could do it so we'll have to revisit this problem.
*/