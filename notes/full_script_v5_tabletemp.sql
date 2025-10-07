DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._EMPDEP
    (
      ssn VARCHAR(100) NOT NULL,
      name VARCHAR(100) NOT NULL,
      phone VARCHAR(100) NOT NULL,
      email VARCHAR(100) NOT NULL,
      dep_name VARCHAR(100) NOT NULL,
      dep_address VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._POSITION
   (
      dep_address VARCHAR(100) NOT NULL,
      city VARCHAR(100) NOT NULL,
      country VARCHAR(100) NOT NULL
   );

ALTER TABLE transducer._EMPDEP ADD PRIMARY KEY (ssn,phone,email);
ALTER TABLE transducer._POSITION ADD PRIMARY KEY (dep_address);

/*
So, this doesn't work, which is a huge problem.
Adding constraints and playing around with references to subset of composite primary key add a bit of complexity to the schema.
But I think a tailor made inclusion dependency constraint between the two tables should work, assuming that _EMPDEP is the main table being referenced by PERSON_CAR

ALTER TABLE transducer._PERSON_CAR 
ADD FOREIGN KEY (ssn) REFERENCES transducer._EMPDEP(ssn);
*/

ALTER TABLE transducer._EMPDEP
ADD FOREIGN KEY (dep_address) REFERENCES transducer._POSITION(dep_address);

CREATE OR REPLACE FUNCTION transducer.check_EMPDEP_mvd_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT DISTINCT r1.ssn, r2.name, r1.phone, r1.email, r2.dep_name, r2.dep_address 
         FROM transducer._EMPDEP AS r1,
         (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.dep_name, NEW.dep_address) AS r2
            WHERE  r1.ssn = r2.ssn 
         EXCEPT
         SELECT *
         FROM transducer._EMPDEP
         ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON PHONE %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


CREATE OR REPLACE FUNCTION transducer.check_EMPDEP_mvd_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS   (SELECT r1.ssn, r1.name, r1.phone, NEW.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.name, NEW.phone, r1.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._EMPDEP)) THEN
      RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', NEW;
      INSERT INTO transducer._EMPDEP 
            (SELECT r1.ssn, r1.name, r1.phone, NEW.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.name, NEW.phone, r1.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._EMPDEP));
      RETURN NEW;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_EMPDEP_FD_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._EMPDEP AS r1,
         (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.dep_name,NEW.dep_address) AS r2
            WHERE  r1.dep_name = r2.dep_name 
         AND r1.dep_address<> r2.dep_address) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN EMPDEP';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_POSITION_FD_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._POSITION AS r1,
         (SELECT NEW.dep_address, NEW.city, NEW.country ) AS r2
            WHERE  r1.city = r2.city 
         AND r1.country<> r2.country) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN POSITION';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;



CREATE OR REPLACE TRIGGER EMPDEP_mvd_trigger_1
BEFORE INSERT ON transducer._EMPDEP
FOR EACH ROW
EXECUTE FUNCTION transducer.check_EMPDEP_mvd_FN_1();

CREATE OR REPLACE TRIGGER EMPDEP_mvd_trigger_2
AFTER INSERT ON transducer._EMPDEP
FOR EACH ROW
EXECUTE FUNCTION transducer.check_EMPDEP_mvd_FN_2();

CREATE TRIGGER EMPDEP_fd_trigger
BEFORE INSERT ON transducer._EMPDEP
FOR EACH ROW
EXECUTE FUNCTION transducer.check_EMPDEP_fd_FN();


INSERT INTO transducer._POSITION (dep_address, city, country) VALUES
('depadd1', 'Paris', 'France'),
('depadd2', 'Roma', 'Italy'),
('depadd3', 'London', 'UK');



INSERT INTO transducer._EMPDEP (ssn, name, phone, email, dep_name, dep_address) VALUES
('ssn1', 'John', 'phone11', 'mail11', 'dep1', 'depadd1'),
('ssn1', 'John', 'phone12', 'mail11','dep1', 'depadd1'),
('ssn2', 'Jane', 'phone21', 'mail21','dep2', 'depadd2'),
('ssn3', 'June', 'phone31', 'mail31','dep3', 'depadd3'),
('ssn3', 'June', 'phone31', 'mail32','dep3', 'depadd3'),
('ssn3', 'June', 'phone32', 'mail31','dep3', 'depadd3'),
('ssn3', 'June', 'phone32', 'mail32','dep3', 'depadd3')
;




/*-----------------------------------------------------------------------------------------------------------------------------------------------------*/

CREATE TABLE transducer._PERSON AS 
SELECT DISTINCT ssn, name, dep_name FROM transducer._EMPDEP;

CREATE TABLE transducer._PERSON_PHONE AS
SELECT DISTINCT ssn, phone FROM transducer._EMPDEP;

CREATE TABLE transducer._PERSON_EMAIL AS 
SELECT DISTINCT ssn, email FROM transducer._EMPDEP;

CREATE TABLE transducer._DEPARTMENT AS
SELECT DISTINCT dep_name, dep_address FROM transducer._EMPDEP;


CREATE TABLE transducer._DEPARTMENT_CITY AS
SELECT DISTINCT dep_address, city FROM transducer._POSITION;

CREATE TABLE transducer._CITY_COUNTRY AS
SELECT DISTINCT city, country FROM transducer._POSITION;



/*BASE CONSTRAINTS*/

ALTER TABLE transducer._PERSON ADD PRIMARY KEY (ssn);

ALTER TABLE transducer._DEPARTMENT ADD PRIMARY KEY (dep_name);

ALTER TABLE transducer._PERSON_PHONE ADD PRIMARY KEY (ssn,phone);

ALTER TABLE transducer._PERSON_EMAIL ADD PRIMARY KEY (ssn,email);


ALTER TABLE transducer._DEPARTMENT_CITY ADD PRIMARY KEY (dep_address);

ALTER TABLE transducer._CITY_COUNTRY ADD PRIMARY KEY (city);



ALTER TABLE transducer._PERSON_PHONE 
ADD FOREIGN KEY (ssn) REFERENCES transducer._PERSON(ssn);

ALTER TABLE transducer._PERSON_EMAIL 
ADD FOREIGN KEY (ssn) REFERENCES transducer._PERSON(ssn);

ALTER TABLE transducer._PERSON
ADD FOREIGN KEY (dep_name) REFERENCES transducer._DEPARTMENT(dep_name);


ALTER TABLE transducer._DEPARTMENT 
ADD FOREIGN KEY (dep_address) REFERENCES transducer._DEPARTMENT_CITY(dep_address);

ALTER TABLE transducer._DEPARTMENT_CITY
ADD FOREIGN KEY (city) REFERENCES transducer._CITY_COUNTRY(city);



/*-----------------------------------------------------------------------------------------------------------------------------------------------------*/

/* S -> T */

CREATE TABLE transducer._EMPDEP_INSERT AS
SELECT * FROM transducer._EMPDEP
WHERE 1<>1;

CREATE TABLE transducer._EMPDEP_DELETE AS
SELECT * FROM transducer._EMPDEP
WHERE 1<>1;

CREATE TABLE transducer._POSITION_INSERT AS
SELECT * FROM transducer._POSITION
WHERE 1<>1;

CREATE TABLE transducer._POSITION_DELETE AS
SELECT * FROM transducer._POSITION
WHERE 1<>1;


CREATE TABLE transducer._EMPDEP_INSERT_JOIN AS
SELECT * FROM transducer._EMPDEP
WHERE 1<>1;

CREATE TABLE transducer._EMPDEP_DELETE_JOIN AS
SELECT * FROM transducer._EMPDEP
WHERE 1<>1;

CREATE TABLE transducer._POSITION_INSERT_JOIN AS
SELECT * FROM transducer._POSITION
WHERE 1<>1;

CREATE TABLE transducer._POSITION_DELETE_JOIN AS
SELECT * FROM transducer._POSITION
WHERE 1<>1;




/** INSERT T -> S **/

CREATE TABLE transducer._PERSON_INSERT AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_DELETE AS
SELECT * FROM transducer._PERSON
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

CREATE TABLE transducer._DEPARTMENT_INSERT AS
SELECT * FROM transducer._DEPARTMENT
WHERE 1<>1;

CREATE TABLE transducer._DEPARTMENT_DELETE AS
SELECT * FROM transducer._DEPARTMENT
WHERE 1<>1;

CREATE TABLE transducer._DEPARTMENT_CITY_INSERT AS
SELECT * FROM transducer._DEPARTMENT_CITY
WHERE 1<>1;

CREATE TABLE transducer._DEPARTMENT_CITY_DELETE AS
SELECT * FROM transducer._DEPARTMENT_CITY
WHERE 1<>1;

CREATE TABLE transducer._CITY_COUNTRY_INSERT AS
SELECT * FROM transducer._CITY_COUNTRY
WHERE 1<>1;

CREATE TABLE transducer._CITY_COUNTRY_DELETE AS
SELECT * FROM transducer._CITY_COUNTRY
WHERE 1<>1;




CREATE TABLE transducer._PERSON_INSERT_JOIN AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_DELETE_JOIN AS
SELECT * FROM transducer._PERSON
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

CREATE TABLE transducer._DEPARTMENT_INSERT_JOIN AS
SELECT * FROM transducer._DEPARTMENT
WHERE 1<>1;

CREATE TABLE transducer._DEPARTMENT_DELETE_JOIN AS
SELECT * FROM transducer._DEPARTMENT
WHERE 1<>1;

CREATE TABLE transducer._DEPARTMENT_CITY_INSERT_JOIN AS
SELECT * FROM transducer._DEPARTMENT_CITY
WHERE 1<>1;

CREATE TABLE transducer._DEPARTMENT_CITY_DELETE_JOIN AS
SELECT * FROM transducer._DEPARTMENT_CITY
WHERE 1<>1;

CREATE TABLE transducer._CITY_COUNTRY_INSERT_JOIN AS
SELECT * FROM transducer._CITY_COUNTRY
WHERE 1<>1;

CREATE TABLE transducer._CITY_COUNTRY_DELETE_JOIN AS
SELECT * FROM transducer._CITY_COUNTRY
WHERE 1<>1;


CREATE TABLE transducer._LOOP (loop_start INT NOT NULL );

/*-----------------------------------------------------------------------------------------------------------------------------------------------------*/

/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_EMPDEP_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
/*
   DELETE FROM transducer._LOOP WHERE loop_id IN(
      SELECT loop_id FROM transducer._LOOP WHERE loop_start = -1 LIMIT 1);
      */
   RETURN NULL;
ELSE
   INSERT INTO transducer._EMPDEP_INSERT VALUES(NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.dep_name, NEW.dep_address);
   RETURN NEW;
END IF;
   END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_POSITION_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._POSITION_INSERT VALUES(NEW.dep_address, NEW.city, NEW.country);
   RETURN NEW;
END IF;
   END;  $$;


CREATE OR REPLACE FUNCTION transducer.source_EMPDEP_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE 
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._EMPDEP_INSERT 
NATURAL LEFT OUTER JOIN transducer._POSITION);

INSERT INTO transducer._EMPDEP_INSERT_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._POSITION_INSERT_JOIN (SELECT dep_address, city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_POSITION_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._POSITION_INSERT 
NATURAL LEFT OUTER JOIN transducer._EMPDEP);

INSERT INTO transducer._EMPDEP_INSERT_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._POSITION_INSERT_JOIN (SELECT dep_address, city, country FROM temp_table);

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
   RAISE NOTICE 'This should conclude with an INSERT on _EMPDEP';

   INSERT INTO transducer._CITY_COUNTRY (SELECT DISTINCT city, country FROM transducer._EMPDEP_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN WHERE ssn IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (city) DO NOTHING;

   INSERT INTO transducer._DEPARTMENT_CITY (SELECT DISTINCT dep_address, city FROM transducer._EMPDEP_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN WHERE ssn IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (dep_address) DO NOTHING;

   INSERT INTO transducer._DEPARTMENT (SELECT DISTINCT dep_name, dep_address FROM transducer._EMPDEP_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN WHERE ssn IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (dep_name) DO NOTHING;

   INSERT INTO transducer._PERSON (SELECT DISTINCT ssn, name, dep_name FROM transducer._EMPDEP_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN WHERE ssn IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (ssn) DO NOTHING;

   INSERT INTO transducer._PERSON_PHONE (SELECT DISTINCT ssn, phone FROM transducer._EMPDEP_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN WHERE ssn IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (ssn,phone) DO NOTHING;

   INSERT INTO transducer._PERSON_EMAIL (SELECT DISTINCT ssn, email FROM transducer._EMPDEP_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN WHERE ssn IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (ssn,email) DO NOTHING;


   DELETE FROM transducer._EMPDEP_INSERT;
   DELETE FROM transducer._POSITION_INSERT;
   DELETE FROM transducer._EMPDEP_INSERT_JOIN;
   DELETE FROM transducer._POSITION_INSERT_JOIN;
   DELETE FROM transducer._loop;
   RETURN NEW;
END IF;
END;  $$;



   /** S->T DELETES **/

CREATE OR REPLACE FUNCTION transducer.source_EMPDEP_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._EMPDEP_DELETE VALUES(OLD.ssn,OLD.name,OLD.phone, OLD.email, OLD.dep_name,OLD.dep_address);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_POSITION_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._POSITION_DELETE VALUES(OLD.dep_address, OLD.city,OLD.country);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_EMPDEP_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._EMPDEP_DELETE
NATURAL LEFT OUTER JOIN transducer._POSITION);

INSERT INTO transducer._EMPDEP_DELETE_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._POSITION_DELETE_JOIN (SELECT dep_address, city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_POSITION_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._POSITION_DELETE
NATURAL LEFT OUTER JOIN transducer._EMPDEP);

INSERT INTO transducer._EMPDEP_DELETE_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._POSITION_DELETE_JOIN (SELECT dep_address, city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.source_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
/*To gain some time, we expect MVDs and sub-tables to be already found*/

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE loop_start = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
ELSE

   /*PHONE*/
   IF EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn 
              EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn AND phone = NEW.phone)) THEN
      DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT ssn, phone FROM transducer._EMPDEP_DELETE_JOIN
                                                         NATURAL LEFT OUTER JOIN transducer._POSITION_DELETE_JOIN);
   END IF;

   /*EMAIL*/
   IF EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn 
              EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn AND email = NEW.email)) THEN
      DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._EMPDEP_DELETE_JOIN
                                                         NATURAL LEFT OUTER JOIN transducer._POSITION_DELETE_JOIN);
   END IF;


   /*At last*/
   IF NOT EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn 
              EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn 
               AND name = NEW.name AND phone = NEW.phone AND email = NEW.email
               AND dep_name = NEW.dep_name AND dep_address = NEW.dep_address)) THEN

      
      DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._EMPDEP_DELETE_JOIN
                                                         NATURAL LEFT OUTER JOIN transducer._POSITION_DELETE_JOIN);




      DELETE FROM transducer._PERSON_PHONE WHERE ssn = NEW.ssn AND phone = NEW.phone;
      DELETE FROM transducer._PERSON_EMAIL WHERE ssn = NEW.ssn AND email = NEW.email;
      DELETE FROM transducer._PERSON WHERE ssn = NEW.ssn AND name = NEW.name AND dep_name = NEW.dep_name;
      DELETE FROM transducer._DEPARTMENT WHERE dep_name = NEW.dep_name AND dep_address = NEW.dep_address;
   END IF;
   DELETE FROM transducer._person_DELETE;
   DELETE FROM transducer._loop;

END IF;
RETURN NEW;
END;  $$;


/**T->S INSERTS **/

CREATE OR REPLACE FUNCTION transducer.target_PERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_INSERT';
   INSERT INTO transducer._PERSON_INSERT VALUES(NEW.ssn, NEW.name, NEW.dep_name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_PHONE_INSERT';
   INSERT INTO transducer._PERSON_PHONE_INSERT VALUES(NEW.ssn, NEW.phone);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_EMAIL_INSERT';
   INSERT INTO transducer._PERSON_EMAIL_INSERT VALUES(NEW.ssn, NEW.email);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _DEPARTMENT_INSERT';
   INSERT INTO transducer._DEPARTMENT_INSERT VALUES(NEW.dep_name, NEW.dep_address);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_CITY_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _DEPARTMENT_INSERT';
   INSERT INTO transducer._DEPARTMENT_INSERT VALUES(NEW.dep_address, NEW.city);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COUNTRY_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _DEPARTMENT_INSERT';
   INSERT INTO transducer._DEPARTMENT_INSERT VALUES(NEW.city, NEW.country);
   RETURN NEW;
END IF;
END;  $$;


   
CREATE OR REPLACE FUNCTION transducer.target_PERSON_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, dep_name, dep_address, city, country
   FROM transducer._PERSON_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._PERSON_PHONE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._PERSON_EMAIL_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._DEPARTMENT_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_CITY_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._DEPARTMENT_CITY_INSERT 
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);
   
INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COUNTRY_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
ssn  VARCHAR(100),
name  VARCHAR(100),
phone  VARCHAR(100),
email  VARCHAR(100),
dep_name  VARCHAR(100),
dep_address  VARCHAR(100),
city  VARCHAR(100),
country  VARCHAR(100)
);

INSERT INTO temp_table (
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._CITY_COUNTRY_INSERT 
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);
   
INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.TARGET_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

SELECT count(*) INTO v_loop from transducer._loop;


IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _EMPDEP';

   create temporary table temp_table_join(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

   INSERT INTO temp_table_join(
   SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country
   FROM transducer._CITY_COUNTRY_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._PERSON_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
      NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
      WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL
      AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL
      AND city IS NOT NULL AND country IS NOT NULL);

      INSERT INTO transducer._POSITION (SELECT dep_address, city, country FROM temp_table_join) ON CONFLICT (dep_address) DO NOTHING;
      INSERT INTO transducer._loop VALUES (-1);
      INSERT INTO transducer._EMPDEP (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table_join) ON CONFLICT (ssn, phone, email) DO NOTHING;
      
      DELETE FROM transducer._PERSON_INSERT;
      DELETE FROM transducer._PERSON_EMAIL_INSERT;
      DELETE FROM transducer._PERSON_PHONE_INSERT;
      DELETE FROM transducer._DEPARTMENT_INSERT;
      DELETE FROM transducer._DEPARTMENT_CITY_INSERT;
      DELETE FROM transducer._CITY_COUNTRY_INSERT;

      DELETE FROM transducer._PERSON_INSERT_JOIN;
      DELETE FROM transducer._PERSON_EMAIL_INSERT_JOIN;
      DELETE FROM transducer._PERSON_PHONE_INSERT_JOIN;
      DELETE FROM transducer._DEPARTMENT_INSERT_JOIN;
      DELETE FROM transducer._DEPARTMENT_CITY_INSERT_JOIN;
      DELETE FROM transducer._CITY_COUNTRY_INSERT_JOIN;

      DELETE FROM transducer._loop;
   DELETE FROM temp_table_join;
   DROP TABLE temp_table_join;
   RETURN NEW;
END IF;
END;  $$;


   /** T->S DELETE **/
   

CREATE OR REPLACE FUNCTION transducer.target_PERSON_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
RETURN NULL;
END IF;
INSERT INTO transducer._PERSON_DELETE VALUES(OLD.ssn, OLD.name, OLD.dep_name);
RETURN NEW;
   END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
RETURN NULL;
END IF;
INSERT INTO transducer._PERSON_PHONE_DELETE VALUES(OLD.ssn, OLD.phone);  
RETURN NEW;
   END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
RETURN NULL;
END IF;
INSERT INTO transducer._PERSON_EMAIL_DELETE VALUES(OLD.ssn, OLD.email);
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
RETURN NULL;
END IF;
INSERT INTO transducer._DEPARTMENT_DELETE VALUES(OLD.dep_name, OLD.dep_address);
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_CITY_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
END IF;
INSERT INTO transducer._DEPARTMENT_CITY_DELETE VALUES(OLD.dep_address, OLD.city);
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COUNTRY_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
END IF;
INSERT INTO transducer._CITY_COUNTRY_DELETE VALUES(OLD.city, OLD.country);
RETURN NEW;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.target_PERSON_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

INSERT INTO temp_table(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._PERSON_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_DELETE_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_DELETE_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_DELETE_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

INSERT INTO temp_table(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._PERSON_PHONE_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_DELETE_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_DELETE_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_DELETE_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

INSERT INTO temp_table(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._PERSON_EMAIL_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_DELETE_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_DELETE_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_DELETE_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

INSERT INTO temp_table(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._DEPARTMENT_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY);

INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_DELETE_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_DELETE_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_DELETE_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPARTMENT_CITY_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

INSERT INTO temp_table(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._DEPARTMENT_CITY_DELETE 
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);
   
INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_DELETE_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_DELETE_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_DELETE_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COUNTRY_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );

INSERT INTO temp_table(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._CITY_COUNTRY_DELETE 
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);
   
INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._DEPARTMENT_DELETE_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._DEPARTMENT_CITY_DELETE_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._CITY_COUNTRY_DELETE_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.TARGET_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
END IF;

RAISE NOTICE 'This should conclude with an DELETE on SOURCE';

create temporary table temp_table_join(
   ssn  VARCHAR(100),
   name  VARCHAR(100),
   phone  VARCHAR(100),
   email  VARCHAR(100),
   dep_name  VARCHAR(100),
   dep_address  VARCHAR(100),
   city  VARCHAR(100),
   country  VARCHAR(100)
   );
INSERT INTO temp_table_join(
SELECT ssn, name, phone, email, dep_name, dep_address, city, country
   FROM transducer._CITY_COUNTRY_DELETE_JOIN
      NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY_DELETE_JOIN
      NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_DELETE_JOIN
      NATURAL LEFT OUTER JOIN transducer._PERSON_DELETE_JOIN
      NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_DELETE_JOIN
      NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_DELETE_JOIN
      WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL
      AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL
      AND city IS NOT NULL AND country IS NOT NULL);
     

/*
IF EXISTS (SELECT ssn, phone, email FROM transducer._EMPDEP WHERE ssn = v_ssn
              EXCEPT (SELECT v_ssn, v_phone, v_email)) THEN
      DELETE FROM transducer._EMPDEP WHERE ssn = v_ssn AND name = v_name AND phone = v_phone AND email = v_email;
ELSE
   
      DELETE FROM transducer._EMPDEP WHERE ssn = v_ssn AND name = v_name AND phone = v_phone AND email = v_email;
      DELETE FROM transducer._POSITION WHERE dep_address = v_dep_address;
   
END IF;
*/


IF EXISTS (SELECT r1.ssn, r1.name, r1.phone, r1.email, r1.dep_name, r1.dep_address, r1.city, r1.country  
   FROM (transducer._POSITION NATURAL LEFT OUTER JOIN transducer._EMPDEP) AS r1, temp_table_join
   WHERE r1.dep_address = temp_table_join.dep_address AND r1.city = temp_table_join.city AND r1.country = temp_table_join.country
   EXCEPT SELECT * FROM temp_table_join) THEN
      DELETE FROM transducer._EMPDEP WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);
ELSE
      DELETE FROM transducer._EMPDEP WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);
      DELETE FROM transducer._POSITION WHERE (dep_address) IN (SELECT dep_address FROM temp_table_join);
END IF;


DELETE FROM transducer._PERSON_DELETE;
DELETE FROM transducer._PERSON_EMAIL_DELETE;
DELETE FROM transducer._PERSON_PHONE_DELETE;
DELETE FROM transducer._DEPARTMENT_DELETE;
DELETE FROM transducer._DEPARTMENT_CITY_DELETE;
DELETE FROM transducer._CITY_COUNTRY_DELETE;

DELETE FROM transducer._PERSON_DELETE_JOIN;
DELETE FROM transducer._PERSON_EMAIL_DELETE_JOIN;
DELETE FROM transducer._PERSON_PHONE_DELETE_JOIN;
DELETE FROM transducer._DEPARTMENT_DELETE_JOIN;
DELETE FROM transducer._DEPARTMENT_CITY_DELETE_JOIN;
DELETE FROM transducer._CITY_COUNTRY_DELETE_JOIN;

DELETE FROM transducer._loop;

DELETE FROM temp_table_join;
DROP TABLE temp_table_join;
RETURN NEW;

END;  $$;

/*-----------------------------------------------------------------------------------------------------------------------------------------------------*/

/** S->T INSERT TRIGGERS **/

CREATE TRIGGER source_EMPDEP_INSERT_trigger
AFTER INSERT ON transducer._EMPDEP
FOR EACH ROW
EXECUTE FUNCTION transducer.source_EMPDEP_INSERT_FN();

CREATE TRIGGER source_POSITION_INSERT_trigger
AFTER INSERT ON transducer._POSITION
FOR EACH ROW
EXECUTE FUNCTION transducer.source_POSITION_INSERT_FN();

CREATE TRIGGER source_EMPDEP_INSERT_JOIN_trigger
AFTER INSERT ON transducer._EMPDEP_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_EMPDEP_INSERT_JOIN_FN();

CREATE TRIGGER source_POSITION_INSERT_JOIN_trigger
AFTER INSERT ON transducer._POSITION_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_POSITION_INSERT_JOIN_FN();

CREATE TRIGGER source_INSERT_trigger_1
AFTER INSERT ON transducer._EMPDEP_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_FN();

CREATE TRIGGER source_INSERT_trigger_2
AFTER INSERT ON transducer._POSITION_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_FN();


/** S->T DELETE TRIGGERS **/

CREATE TRIGGER source_EMPDEP_DELETE_trigger
AFTER DELETE ON transducer._EMPDEP
FOR EACH ROW
EXECUTE FUNCTION transducer.source_EMPDEP_DELETE_FN();

CREATE TRIGGER source_POSITION_DELETE_trigger
AFTER DELETE ON transducer._POSITION
FOR EACH ROW
EXECUTE FUNCTION transducer.source_POSITION_DELETE_FN();

CREATE TRIGGER source_EMPDEP_DELETE_JOIN_trigger
AFTER INSERT ON transducer._EMPDEP_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_EMPDEP_DELETE_JOIN_FN();

CREATE TRIGGER source_POSITION_DELETE_JOIN_trigger
AFTER INSERT ON transducer._POSITION_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_POSITION_DELETE_JOIN_FN();

CREATE TRIGGER source_DELETE_trigger_1
AFTER INSERT ON transducer._EMPDEP_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_DELETE_FN();

CREATE TRIGGER source_DELETE_trigger_2
AFTER INSERT ON transducer._POSITION_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_DELETE_FN();

/** T->S INSERT **/

CREATE TRIGGER target_PERSON_INSERT_trigger
AFTER INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_INSERT_FN();

CREATE TRIGGER target_PERSON_PHONE_INSERT_trigger
AFTER INSERT ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_FN();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_trigger
AFTER INSERT ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_FN();

CREATE TRIGGER target_DEPARTMENT_INSERT_trigger
AFTER INSERT ON transducer._DEPARTMENT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_INSERT_FN();

CREATE TRIGGER target_DEPARTMENT_CITY_INSERT_trigger
AFTER INSERT ON transducer._DEPARTMENT_CITY
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_CITY_INSERT_FN();

CREATE TRIGGER target_CITY_COUNTRY_INSERT_trigger
AFTER INSERT ON transducer._CITY_COUNTRY
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COUNTRY_INSERT_FN();



CREATE TRIGGER target_PERSON_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_INSERT_JOIN_FN();

CREATE TRIGGER target_PERSON_PHONE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_FN();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_FN();

CREATE TRIGGER target_DEPARTMENT_INSERT_JOIN_trigger
AFTER INSERT ON transducer._DEPARTMENT_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_INSERT_JOIN_FN();

CREATE TRIGGER target_DEPARTMENT_CITY_INSERT_JOIN_trigger
AFTER INSERT ON transducer._DEPARTMENT_CITY_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_CITY_INSERT_JOIN_FN();

CREATE TRIGGER target_CITY_COUNTRY_INSERT_JOIN_trigger
AFTER INSERT ON transducer._CITY_COUNTRY_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COUNTRY_INSERT_JOIN_FN();





CREATE TRIGGER target_INSERT_trigger_1
AFTER INSERT ON transducer._PERSON_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_FN();

CREATE TRIGGER target_INSERT_trigger_2
AFTER INSERT ON transducer._PERSON_PHONE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_FN();

CREATE TRIGGER target_INSERT_trigger_3
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_FN();

CREATE TRIGGER target_INSERT_trigger_4
AFTER INSERT ON transducer._DEPARTMENT_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_FN();

CREATE TRIGGER target_INSERT_trigger_5
AFTER INSERT ON transducer._DEPARTMENT_CITY_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_FN();

CREATE TRIGGER target_INSERT_trigger_6
AFTER INSERT ON transducer._CITY_COUNTRY_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_FN();


/** T->S DELETE **/

CREATE TRIGGER target_PERSON_DELETE_trigger
AFTER DELETE ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_DELETE_FN();

CREATE TRIGGER target_PERSON_PHONE_DELETE_trigger
AFTER DELETE ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_DELETE_FN();

CREATE TRIGGER target_PERSON_EMAIL_DELETE_trigger
AFTER DELETE ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_DELETE_FN();

CREATE TRIGGER target_DEPARTMENT_DELETE_trigger
AFTER DELETE ON transducer._DEPARTMENT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_DELETE_FN();

CREATE TRIGGER target_DEPARTMENT_CITY_DELETE_trigger
AFTER DELETE ON transducer._DEPARTMENT_CITY
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_CITY_DELETE_FN();

CREATE TRIGGER target_CITY_COUNTRY_DELETE_trigger
AFTER DELETE ON transducer._CITY_COUNTRY
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COUNTRY_DELETE_FN();


CREATE TRIGGER target_PERSON_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_DELETE_JOIN_FN();

CREATE TRIGGER target_PERSON_PHONE_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_DELETE_JOIN_FN();

CREATE TRIGGER target_PERSON_EMAIL_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_DELETE_JOIN_FN();

CREATE TRIGGER target_DEPARTMENT_DELETE_JOIN_trigger
AFTER INSERT ON transducer._DEPARTMENT_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_DELETE_JOIN_FN();

CREATE TRIGGER target_DEPARTMENT_CITY_DELETE_JOIN_trigger
AFTER INSERT ON transducer._DEPARTMENT_CITY_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPARTMENT_CITY_DELETE_JOIN_FN();

CREATE TRIGGER target_CITY_COUNTRY_DELETE_JOIN_trigger
AFTER INSERT ON transducer._CITY_COUNTRY_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COUNTRY_DELETE_JOIN_FN();


CREATE TRIGGER target_DELETE_trigger_1
AFTER INSERT ON transducer._PERSON_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_FN();

CREATE TRIGGER target_DELETE_trigger_2
AFTER INSERT ON transducer._PERSON_PHONE_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_FN();

CREATE TRIGGER target_DELETE_trigger_3
AFTER INSERT ON transducer._PERSON_EMAIL_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_FN();

CREATE TRIGGER target_DELETE_trigger_4
AFTER INSERT ON transducer._DEPARTMENT_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_FN();

CREATE TRIGGER target_DELETE_trigger_5
AFTER INSERT ON transducer._DEPARTMENT_CITY_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_FN();

CREATE TRIGGER target_DELETE_trigger_6
AFTER INSERT ON transducer._CITY_COUNTRY_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_FN();