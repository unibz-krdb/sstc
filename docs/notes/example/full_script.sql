DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._PERSON
    (
      ssn VARCHAR(100) NOT NULL,
      empid VARCHAR(100),
      name VARCHAR(100) NOT NULL,
      hdate VARCHAR(100),
      phone VARCHAR(100) NOT NULL,
      email VARCHAR(100) NOT NULL,
      dept VARCHAR(100),
      manager VARCHAR(100)
    );


ALTER TABLE transducer._PERSON ADD PRIMARY KEY (ssn,phone,email);




CREATE OR REPLACE FUNCTION transducer.check_PERSON_mvd_fn_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT DISTINCT r1.ssn, r2.empid, r2.name, r2.hdate, r1.phone, r1.email, r2.dept, r2.manager 
         FROM transducer._PERSON AS r1,
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS r2
            WHERE  r1.ssn = r2.ssn 
         EXCEPT
         SELECT *
         FROM transducer._PERSON
         ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON PHONE AND EMAIL %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_mvd_fn_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS   (SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._PERSON)) THEN
      RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', NEW;
      INSERT INTO transducer._PERSON 
            (SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._PERSON));
      RETURN NEW;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_IND_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF (NEW.manager IS NULL) THEN
      RETURN NEW;
   END IF;
   IF(NEW.manager = NEW.ssn) THEN
      RETURN NEW;
   END IF;
   IF EXISTS (SELECT DISTINCT NEW.manager 
            FROM transducer._person
         EXCEPT(
         SELECT ssn AS manager
         FROM transducer._person
         UNION
         SELECT NEW.ssn as manager)) THEN
         RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE INC1 CONSTRAINT';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
         WHERE (R2.empid IS NOT NULL AND R2.hdate IS NOT NULL 
            AND R1.empid = R2.empid 
            AND R1.hdate <> R2.hdate) 
            OR (R2.empid IS NULL AND R2.hdate IS NOT NULL) 
            OR (R2.empid IS NOT NULL AND R2.hdate IS NULL)) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> hdate %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


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

CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_3()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
         WHERE (R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL 
            AND R1.dept = R2.dept 
            AND R1.manager <> R2.manager) 
            OR (R2.dept IS NULL AND R2.manager IS NOT NULL) 
            OR (R2.dept IS NOT NULL AND R2.manager IS NULL)) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT dept -> manager %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;



CREATE OR REPLACE TRIGGER PERSON_mvd_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_mvd_fn_1();

CREATE OR REPLACE TRIGGER PERSON_mvd_trigger_2
AFTER INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_mvd_fn_2();

CREATE TRIGGER PERSON_cfd_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_cfd_fn_1();



CREATE TRIGGER PERSON_cfd_trigger_2
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_cfd_fn_2();

CREATE TRIGGER PERSON_cfd_trigger_3
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_cfd_fn_3();



CREATE TRIGGER PERSON_IND_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_ind_fn_1();



INSERT INTO transducer._PERSON (ssn, empid, name, hdate, phone, email, dept, manager) VALUES
('ssn1', 'emp1', 'June', 'hdate1', 'phone11', 'mail11', 'dep1', 'ssn1'),
('ssn2', 'emp2', 'Jovial', 'hdate2', 'phone21', 'mail21', NULL, NULL),
('ssn3', NULL, 'Jord', NULL, 'phone31', 'mail31', NULL, NULL)
;

/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

CREATE TABLE transducer._P AS 
   SELECT DISTINCT ssn, name FROM transducer._PERSON;

CREATE TABLE transducer._PE AS
   SELECT DISTINCT ssn, empid FROM transducer._PERSON
   WHERE empid IS NOT NULL AND hdate IS NOT NULL;

CREATE TABLE transducer._PED AS
   SELECT DISTINCT ssn, empid FROM transducer._PERSON
   WHERE empid IS NOT NULL AND dept IS NOT NULL;

CREATE TABLE transducer._PE_HDATE AS
   SELECT DISTINCT empid, hdate FROM transducer._PERSON
   WHERE empid IS NOT NULL AND hdate IS NOT NULL;

CREATE TABLE transducer._PED_DEPT AS
   SELECT DISTINCT empid, dept FROM transducer._PERSON
   WHERE empid IS NOT NULL AND dept IS NOT NULL;

CREATE TABLE transducer._DEPT_MANAGER AS
   SELECT DISTINCT dept, manager FROM transducer._PERSON
   WHERE dept IS NOT NULL AND manager IS NOT NULL;

CREATE TABLE transducer._PERSON_PHONE AS
SELECT DISTINCT ssn, phone FROM transducer._PERSON;

CREATE TABLE transducer._PERSON_EMAIL AS 
SELECT DISTINCT ssn, email FROM transducer._PERSON;





/*BASE CONSTRAINTS*/

ALTER TABLE transducer._P ADD PRIMARY KEY (ssn);

ALTER TABLE transducer._PE ADD PRIMARY KEY (empid);

ALTER TABLE transducer._PED ADD PRIMARY KEY (empid);

ALTER TABLE transducer._PE_HDATE ADD PRIMARY KEY (empid);

ALTER TABLE transducer._PED_DEPT ADD PRIMARY KEY (dept);

ALTER TABLE transducer._DEPT_MANAGER ADD PRIMARY KEY (dept);

ALTER TABLE transducer._PERSON_PHONE ADD PRIMARY KEY (ssn,phone);

ALTER TABLE transducer._PERSON_EMAIL ADD PRIMARY KEY (ssn,email);





ALTER TABLE transducer._PE
ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);

ALTER TABLE transducer._PED
ADD FOREIGN KEY (empid) REFERENCES transducer._PE(empid);

ALTER TABLE transducer._PE_HDATE
ADD FOREIGN KEY (empid) REFERENCES transducer._PE(empid);

ALTER TABLE transducer._PED_DEPT
ADD FOREIGN KEY (empid) REFERENCES transducer._PED(empid);

/*
ALTER TABLE transducer._DEPT_MANAGER
ADD FOREIGN KEY (dept) REFERENCES transducer._PED_DEPT(dept);
*/

ALTER TABLE transducer._PED_DEPT
ADD FOREIGN KEY (dept) REFERENCES transducer._DEPT_MANAGER(dept);

ALTER TABLE transducer._DEPT_MANAGER
ADD FOREIGN KEY (manager) REFERENCES transducer._P(ssn);

ALTER TABLE transducer._PERSON_PHONE 
ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);

ALTER TABLE transducer._PERSON_EMAIL 
ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);




/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

/* S -> T */

CREATE TABLE transducer._PERSON_INSERT AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_DELETE AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;


CREATE TABLE transducer._PERSON_INSERT_JOIN AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_DELETE_JOIN AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;






/** INSERT T -> S **/

CREATE TABLE transducer._P_INSERT AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._P_DELETE AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._PE_INSERT AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PE_DELETE AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PED_INSERT AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PED_DELETE AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_INSERT AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_DELETE AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_INSERT AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_DELETE AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_INSERT AS
SELECT * FROM transducer._DEPT_MANAGER
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_DELETE AS
SELECT * FROM transducer._DEPT_MANAGER
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_INSERT AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_DELETE AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_INSERT AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_DELETE AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;


CREATE TABLE transducer._P_INSERT_JOIN AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._P_DELETE_JOIN AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._PE_INSERT_JOIN AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PE_DELETE_JOIN AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PED_INSERT_JOIN AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PED_DELETE_JOIN AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_INSERT_JOIN AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_DELETE_JOIN AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_INSERT_JOIN AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_DELETE_JOIN AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_INSERT_JOIN AS
SELECT * FROM transducer._DEPT_MANAGER
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_DELETE_JOIN AS
SELECT * FROM transducer._DEPT_MANAGER
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_INSERT_JOIN AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_DELETE_JOIN AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_INSERT_JOIN AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_DELETE_JOIN AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;


CREATE TABLE transducer._LOOP (
loop_start INT NOT NULL );



/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */


/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_PERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._PERSON_INSERT VALUES(NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager);
   RETURN NEW;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.source_PERSON_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, empid, name, hdate, phone, email, dept, manager
FROM transducer._PERSON_INSERT);

INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;

END;  $$;


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

/** T->S INSERTS **/

CREATE OR REPLACE FUNCTION transducer.target_P_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _P';
   INSERT INTO transducer._P_INSERT VALUES(NEW.ssn, NEW.name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_P_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PE';
   INSERT INTO transducer._PE_INSERT VALUES(NEW.ssn, NEW.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PE_HDATE';
   INSERT INTO transducer._PE_HDATE_INSERT VALUES(NEW.empid, NEW.hdate);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_HDATE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PED';
   INSERT INTO transducer._PED_INSERT VALUES(NEW.ssn, NEW.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PED_DEPT';
   INSERT INTO transducer._PED_DEPT_INSERT VALUES(NEW.empid, NEW.dept);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_DEPT_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _DEPT_MANAGER';
   INSERT INTO transducer._DEPT_MANAGER_INSERT VALUES(NEW.dept, NEW.manager);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._DEPT_MANAGER_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_PHONE';
   INSERT INTO transducer._PERSON_PHONE_INSERT VALUES(NEW.ssn, NEW.phone);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_PHONE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_EMAIL';
   INSERT INTO transducer._PERSON_EMAIL_INSERT VALUES(NEW.ssn, NEW.email);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_EMAIL_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


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

/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */



/** S->T INSERT TRIGGERS **/

CREATE TRIGGER source_PERSON_INSERT_trigger
AFTER INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.source_PERSON_INSERT_fn();

CREATE TRIGGER source_PERSON_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_PERSON_INSERT_JOIN_fn();

CREATE TRIGGER source_INSERT_trigger_1
AFTER INSERT ON transducer._PERSON_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_fn();




/** T->S INSERT **/

CREATE TRIGGER target_P_INSERT_trigger
AFTER INSERT ON transducer._P
FOR EACH ROW
EXECUTE FUNCTION transducer.target_P_INSERT_fn();

CREATE TRIGGER target_PE_INSERT_trigger
AFTER INSERT ON transducer._PE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_INSERT_fn();

CREATE TRIGGER target_PE_HDATE_INSERT_trigger
AFTER INSERT ON transducer._PE_HDATE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_HDATE_INSERT_fn();

CREATE TRIGGER target_PED_INSERT_trigger
AFTER INSERT ON transducer._PED
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_INSERT_fn();

CREATE TRIGGER target_PED_DEPT_INSERT_trigger
AFTER INSERT ON transducer._PED_DEPT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DEPT_INSERT_fn();

CREATE TRIGGER target_DEPT_MANAGER_INSERT_trigger
AFTER INSERT ON transducer._DEPT_MANAGER
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPT_MANAGER_INSERT_fn();

CREATE TRIGGER target_PERSON_PHONE_INSERT_trigger
AFTER INSERT ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_fn();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_trigger
AFTER INSERT ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_fn();




CREATE TRIGGER target_P_INSERT_JOIN_trigger
AFTER INSERT ON transducer._P_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_P_INSERT_JOIN_fn();

CREATE TRIGGER target_PE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_INSERT_JOIN_fn();

CREATE TRIGGER target_PE_HDATE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PE_HDATE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_HDATE_INSERT_JOIN_fn();

CREATE TRIGGER target_PED_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PED_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_INSERT_JOIN_fn();

CREATE TRIGGER target_PED_DEPT_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PED_DEPT_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DEPT_INSERT_JOIN_fn();

CREATE TRIGGER target_DEPT_MANAGER_INSERT_JOIN_trigger
AFTER INSERT ON transducer._DEPT_MANAGER_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPT_MANAGER_INSERT_JOIN_fn();

CREATE TRIGGER target_PERSON_PHONE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_fn();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_fn();


CREATE TRIGGER target_INSERT_trigger_1
AFTER INSERT ON transducer._P_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_2
AFTER INSERT ON transducer._PE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_3
AFTER INSERT ON transducer._PE_HDATE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_4
AFTER INSERT ON transducer._PED_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_5
AFTER INSERT ON transducer._PED_DEPT_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_6
AFTER INSERT ON transducer._DEPT_MANAGER_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_7
AFTER INSERT ON transducer._PERSON_PHONE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_8
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();







