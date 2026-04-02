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
	IF EXISTS 	(SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
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


