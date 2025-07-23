DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

/****************************/
/* SOURCE TABLE DEFINITIONS */
/****************************/

--------------------------
-- transducer._position --
--------------------------

-- create
CREATE TABLE transducer._position (
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100),
	PRIMARY KEY (dep_address)
);

-- insert table
CREATE TABLE transducer._position_INSERT AS
SELECT * FROM transducer._position
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._position_INSERT_JOIN AS
SELECT * FROM transducer._position
WHERE 1<>1;

-- constraint 1 of 1
CREATE OR REPLACE FUNCTION transducer._position_fd_1_insert_fn()
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
CREATE TRIGGER transducer__position_fd_1_insert_trigger
BEFORE INSERT ON transducer._position
FOR EACH ROW
EXECUTE FUNCTION transducer._position_fd_1_insert_fn();

------------------------
-- transducer._empdep --
------------------------

-- create
CREATE TABLE transducer._empdep (
	ssn VARCHAR(100),
	name VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	PRIMARY KEY (ssn, phone, email),
	FOREIGN KEY (dep_address) REFERENCES transducer._position (dep_address)
);

-- insert table
CREATE TABLE transducer._empdep_INSERT AS
SELECT * FROM transducer._empdep
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._empdep_INSERT_JOIN AS
SELECT * FROM transducer._empdep
WHERE 1<>1;

-- constraint 1 of 3
CREATE OR REPLACE FUNCTION transducer._empdep_mvd_1_insert_fn()
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
CREATE TRIGGER transducer__empdep_mvd_1_insert_trigger
BEFORE INSERT ON transducer._empdep
FOR EACH ROW
EXECUTE FUNCTION transducer._empdep_mvd_1_insert_fn();

-- constraint 2 of 3
CREATE OR REPLACE FUNCTION transducer._empdep_fd_1_insert_fn()
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
CREATE TRIGGER transducer__empdep_fd_1_insert_trigger
BEFORE INSERT ON transducer._empdep
FOR EACH ROW
EXECUTE FUNCTION transducer._empdep_fd_1_insert_fn();

-- constraint 3 of 3
CREATE OR REPLACE FUNCTION transducer._empdep_mvd_2_insert_fn()
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
CREATE TRIGGER transducer__empdep_mvd_2_insert_trigger
AFTER INSERT ON transducer._empdep
FOR EACH ROW
EXECUTE FUNCTION transducer._empdep_mvd_2_insert_fn();

/****************************/
/* TARGET TABLE DEFINITIONS */
/****************************/

------------------------------
-- transducer._city_country --
------------------------------

-- create
CREATE TABLE transducer._city_country (
	city VARCHAR(100),
	country VARCHAR(100),
	PRIMARY KEY (city)
);

-- insert table
CREATE TABLE transducer._city_country_INSERT AS
SELECT * FROM transducer._city_country
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._city_country_INSERT_JOIN AS
SELECT * FROM transducer._city_country
WHERE 1<>1;

-- no constraints

---------------------------------
-- transducer._department_city --
---------------------------------

-- create
CREATE TABLE transducer._department_city (
	dep_address VARCHAR(100),
	city VARCHAR(100),
	PRIMARY KEY (dep_address),
	FOREIGN KEY (city) REFERENCES transducer._city_country (city)
);

-- insert table
CREATE TABLE transducer._department_city_INSERT AS
SELECT * FROM transducer._department_city
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._department_city_INSERT_JOIN AS
SELECT * FROM transducer._department_city
WHERE 1<>1;

-- no constraints

----------------------------
-- transducer._department --
----------------------------

-- create
CREATE TABLE transducer._department (
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	PRIMARY KEY (dep_name),
	FOREIGN KEY (dep_address) REFERENCES transducer._department_city (dep_address)
);

-- insert table
CREATE TABLE transducer._department_INSERT AS
SELECT * FROM transducer._department
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._department_INSERT_JOIN AS
SELECT * FROM transducer._department
WHERE 1<>1;

-- no constraints

------------------------
-- transducer._person --
------------------------

-- create
CREATE TABLE transducer._person (
	ssn VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	PRIMARY KEY (ssn),
	FOREIGN KEY (dep_name) REFERENCES transducer._department (dep_name)
);

-- insert table
CREATE TABLE transducer._person_INSERT AS
SELECT * FROM transducer._person
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._person_INSERT_JOIN AS
SELECT * FROM transducer._person
WHERE 1<>1;

-- no constraints

------------------------------
-- transducer._person_email --
------------------------------

-- create
CREATE TABLE transducer._person_email (
	ssn VARCHAR(100),
	email VARCHAR(100),
	PRIMARY KEY (ssn, email),
	FOREIGN KEY (ssn) REFERENCES transducer._person (ssn)
);

-- insert table
CREATE TABLE transducer._person_email_INSERT AS
SELECT * FROM transducer._person_email
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._person_email_INSERT_JOIN AS
SELECT * FROM transducer._person_email
WHERE 1<>1;

-- no constraints

------------------------------
-- transducer._person_phone --
------------------------------

-- create
CREATE TABLE transducer._person_phone (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	PRIMARY KEY (ssn, phone),
	FOREIGN KEY (ssn) REFERENCES transducer._person (ssn)
);

-- insert table
CREATE TABLE transducer._person_phone_INSERT AS
SELECT * FROM transducer._person_phone
WHERE 1<>1;

-- insert join table
CREATE TABLE transducer._person_phone_INSERT_JOIN AS
SELECT * FROM transducer._person_phone
WHERE 1<>1;

-- no constraints

/******************************/
/* TABLE FUNCTIONS & TRIGGERS */
/******************************/

-- loop prevention mechanism
CREATE TABLE transducer._LOOP (loop_start INT NOT NULL );

--------------------------
-- transducer._position --
--------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._position_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._position_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = -1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._position_INSERT VALUES(new.dep_address, new.city, new.country);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__position_INSERT_trigger
AFTER INSERT ON transducer._position
FOR EACH ROW
EXECUTE FUNCTION transducer._position_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._position_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._position_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	name VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._POSITION_INSERT
NATURAL LEFT OUTER JOIN transducer._EMPDEP
WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._empdep_INSERT_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._position_INSERT_JOIN (SELECT dep_address, city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _position_INSERT_JOIN_trigger
AFTER INSERT ON transducer._position_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._position_INSERT_JOIN_fn();
        
------------------------
-- transducer._empdep --
------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._empdep_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._empdep_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = -1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._empdep_INSERT VALUES(new.ssn, new.name, new.phone, new.email, new.dep_name, new.dep_address);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__empdep_INSERT_trigger
AFTER INSERT ON transducer._empdep
FOR EACH ROW
EXECUTE FUNCTION transducer._empdep_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._empdep_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._empdep_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	name VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._EMPDEP_INSERT
NATURAL LEFT OUTER JOIN transducer._POSITION
WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._empdep_INSERT_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._position_INSERT_JOIN (SELECT dep_address, city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _empdep_INSERT_JOIN_trigger
AFTER INSERT ON transducer._empdep_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._empdep_INSERT_JOIN_fn();
        
------------------------------
-- transducer._city_country --
------------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._city_country_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._city_country_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._city_country_INSERT VALUES(new.city, new.country);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__city_country_INSERT_trigger
AFTER INSERT ON transducer._city_country
FOR EACH ROW
EXECUTE FUNCTION transducer._city_country_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._city_country_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._city_country_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
FROM transducer._CITY_COUNTRY_INSERT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _city_country_INSERT_JOIN_trigger
AFTER INSERT ON transducer._city_country_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._city_country_INSERT_JOIN_fn();
        
---------------------------------
-- transducer._department_city --
---------------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._department_city_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._department_city_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._department_city_INSERT VALUES(new.dep_address, new.city);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__department_city_INSERT_trigger
AFTER INSERT ON transducer._department_city
FOR EACH ROW
EXECUTE FUNCTION transducer._department_city_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._department_city_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._department_city_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
FROM transducer._DEPARTMENT_CITY_INSERT
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _department_city_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_city_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._department_city_INSERT_JOIN_fn();
        
----------------------------
-- transducer._department --
----------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._department_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._department_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._department_INSERT VALUES(new.dep_name, new.dep_address);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__department_INSERT_trigger
AFTER INSERT ON transducer._department
FOR EACH ROW
EXECUTE FUNCTION transducer._department_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._department_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._department_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
FROM transducer._DEPARTMENT_INSERT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _department_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._department_INSERT_JOIN_fn();
        
------------------------
-- transducer._person --
------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._person_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._person_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._person_INSERT VALUES(new.ssn, new.name, new.dep_name);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__person_INSERT_trigger
AFTER INSERT ON transducer._person
FOR EACH ROW
EXECUTE FUNCTION transducer._person_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._person_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._person_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
   FROM transducer._PERSON_INSERT
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _person_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._person_INSERT_JOIN_fn();
        
------------------------------
-- transducer._person_email --
------------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._person_email_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._person_email_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._person_email_INSERT VALUES(new.ssn, new.email);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__person_email_INSERT_trigger
AFTER INSERT ON transducer._person_email
FOR EACH ROW
EXECUTE FUNCTION transducer._person_email_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._person_email_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._person_email_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
FROM transducer._PERSON_EMAIL_INSERT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _person_email_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_email_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._person_email_INSERT_JOIN_fn();
        
------------------------------
-- transducer._person_phone --
------------------------------

-- insert function
CREATE OR REPLACE FUNCTION transducer._person_phone_INSERT_fn()
   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
   BEGIN
   RAISE NOTICE 'Function transducer._person_phone_INSERT_fn called';
   IF EXISTS (SELECT * FROM transducer._loop where loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._person_phone_INSERT VALUES(new.ssn, new.phone);
      RETURN NEW;
   END IF;
END;  $$;

-- insert trigger
CREATE TRIGGER transducer__person_phone_INSERT_trigger
AFTER INSERT ON transducer._person_phone
FOR EACH ROW
EXECUTE FUNCTION transducer._person_phone_INSERT_fn();

-- insert join function
CREATE OR REPLACE FUNCTION transducer._person_phone_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer._person_phone_INSERT_JOIN_FN called';

create temporary table temp_table (
	ssn VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	name VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);
INSERT INTO temp_table (SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
FROM transducer._PERSON_PHONE_INSERT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
INSERT INTO transducer._LOOP VALUES (1);
INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;

RETURN NEW;
END;  $$;

-- insert join trigger
CREATE TRIGGER _person_phone_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_phone_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer._person_phone_INSERT_JOIN_fn();
        

/**************************************/
/* SOURCE/TARGET FUNCTIONS & TRIGGERS */
/**************************************/

------------
-- insert --
------------

-- S -> T
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
   RAISE NOTICE 'This should conclude with an INSERT on _EMPDEP';
        
create temporary table temp_table_join (
	ssn VARCHAR(100),
	name VARCHAR(100),
	phone VARCHAR(100),
	email VARCHAR(100),
	dep_name VARCHAR(100),
	dep_address VARCHAR(100),
	city VARCHAR(100),
	country VARCHAR(100)
);

INSERT INTO temp_table_join(SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
FROM transducer._CITY_COUNTRY_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL);

INSERT INTO transducer._position (SELECT dep_address, city, country FROM temp_table_join) ON CONFLICT (dep_address) DO NOTHING;
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._empdep (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table_join) ON CONFLICT (ssn, phone, email) DO NOTHING;

DELETE FROM transducer._person_phone_INSERT;
DELETE FROM transducer._person_email_INSERT;
DELETE FROM transducer._person_INSERT;
DELETE FROM transducer._department_INSERT;
DELETE FROM transducer._department_city_INSERT;
DELETE FROM transducer._city_country_INSERT;

DELETE FROM transducer._person_phone_INSERT_JOIN;
DELETE FROM transducer._person_email_INSERT_JOIN;
DELETE FROM transducer._person_INSERT_JOIN;
DELETE FROM transducer._department_INSERT_JOIN;
DELETE FROM transducer._department_city_INSERT_JOIN;
DELETE FROM transducer._city_country_INSERT_JOIN;

DELETE FROM transducer._loop;
DELETE FROM temp_table_join;
DROP TABLE temp_table_join;

RETURN NEW;
END IF;
END;    $$;

CREATE TRIGGER target_insert__person_phone_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_phone_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__department_city_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_city_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__department_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__person_email_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_email_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__city_country_INSERT_JOIN_trigger
AFTER INSERT ON transducer._city_country_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__person_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

-- T -> S
CREATE OR REPLACE FUNCTION transducer.source_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Function transducer.source_insert_fn called';
IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _EMPDEP';

	INSERT INTO transducer._city_country (SELECT DISTINCT city, country
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (city) DO NOTHING;

	INSERT INTO transducer._department_city (SELECT DISTINCT dep_address, city
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (dep_address) DO NOTHING;

	INSERT INTO transducer._department (SELECT DISTINCT dep_name, dep_address
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (dep_name) DO NOTHING;

	INSERT INTO transducer._person (SELECT DISTINCT ssn, name, dep_name
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (ssn) DO NOTHING;

	INSERT INTO transducer._person_email (SELECT DISTINCT ssn, email
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (ssn,email) DO NOTHING;

	INSERT INTO transducer._person_phone (SELECT DISTINCT ssn, phone
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL) ON CONFLICT (ssn,phone) DO NOTHING;

	DELETE FROM transducer._empdep_INSERT;
	DELETE FROM transducer._position_INSERT;
	DELETE FROM transducer._empdep_INSERT_JOIN;
	DELETE FROM transducer._position_INSERT_JOIN;
	DELETE FROM transducer._loop;
RETURN NEW;
END IF;
END;  $$;

CREATE TRIGGER source_insert__empdep_INSERT_JOIN_trigger
AFTER INSERT ON transducer._empdep_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_insert_fn();

CREATE TRIGGER source_insert__position_INSERT_JOIN_trigger
AFTER INSERT ON transducer._position_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_insert_fn();

