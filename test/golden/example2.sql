DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._loop (loop_start INT NOT NULL);


CREATE TABLE transducer._person_source (
    ssn VARCHAR(100) NOT NULL,    empid VARCHAR(100),    name VARCHAR(100) NOT NULL,    hdate VARCHAR(100),    phone VARCHAR(100) NOT NULL,    email VARCHAR(100) NOT NULL,    dept VARCHAR(100),    manager VARCHAR(100),    PRIMARY KEY (ssn, phone, email)
);

CREATE TABLE transducer._p (
    ssn VARCHAR(100) NOT NULL,    name VARCHAR(100) NOT NULL,    PRIMARY KEY (ssn)
);

CREATE TABLE transducer._personphone (
    ssn VARCHAR(100) NOT NULL,    phone VARCHAR(100) NOT NULL,    PRIMARY KEY (ssn, phone)
);

CREATE TABLE transducer._personemail (
    ssn VARCHAR(100) NOT NULL,    email VARCHAR(100) NOT NULL,    PRIMARY KEY (ssn, email)
);

CREATE TABLE transducer._pe (
    ssn VARCHAR(100) NOT NULL,    empid VARCHAR(100),    PRIMARY KEY (empid)
);

CREATE TABLE transducer._pe_hdate (
    empid VARCHAR(100),    hdate VARCHAR(100),    PRIMARY KEY (empid)
);

CREATE TABLE transducer._ped (
    ssn VARCHAR(100) NOT NULL,    empid VARCHAR(100),    PRIMARY KEY (empid)
);

CREATE TABLE transducer._peddept (
    empid VARCHAR(100),    dept VARCHAR(100),    PRIMARY KEY (empid)
);

CREATE TABLE transducer._deptmanager (
    dept VARCHAR(100),    manager VARCHAR(100),    PRIMARY KEY (dept)
);


ALTER TABLE transducer._personphone ADD FOREIGN KEY (ssn) REFERENCES transducer._p (ssn);
ALTER TABLE transducer._personemail ADD FOREIGN KEY (ssn) REFERENCES transducer._p (ssn);
ALTER TABLE transducer._pe_hdate ADD FOREIGN KEY (empid) REFERENCES transducer._pe (empid);
ALTER TABLE transducer._peddept ADD FOREIGN KEY (empid) REFERENCES transducer._ped (empid);
ALTER TABLE transducer._pe ADD FOREIGN KEY (ssn) REFERENCES transducer._p (ssn);
ALTER TABLE transducer._ped ADD FOREIGN KEY (empid) REFERENCES transducer._pe (empid);
ALTER TABLE transducer._deptmanager ADD FOREIGN KEY (manager) REFERENCES transducer._p (ssn);

CREATE OR REPLACE FUNCTION transducer.check_person_source_mvd_check_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT DISTINCT r1.ssn, r2.empid, r2.name, r2.hdate, r1.phone, r1.email, r2.dept, r2.manager
        FROM transducer._person_source AS r1,
        (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS r2
        WHERE r1.ssn = r2.ssn
        EXCEPT
        SELECT *
        FROM transducer._person_source
    ) THEN
        RAISE EXCEPTION 'MVD constraint violation on person_source';
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER person_source_mvd_check_trigger
BEFORE INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.check_person_source_mvd_check_fn();


CREATE OR REPLACE FUNCTION transducer.check_person_source_mvd_grounding_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (
        SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
        FROM transducer._person_source AS r1
        WHERE r1.ssn = NEW.ssn
        UNION
        SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
        FROM transducer._person_source AS r1
        WHERE r1.ssn = NEW.ssn
        EXCEPT
        (SELECT * FROM transducer._person_source)
    ) THEN
        RAISE NOTICE 'MVD grounding: tuple % leads to additional tuples', NEW;
        INSERT INTO transducer._person_source (
            SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
            FROM transducer._person_source AS r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
            FROM transducer._person_source AS r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT
            (SELECT * FROM transducer._person_source)
        );
        RETURN NEW;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER person_source_mvd_grounding_trigger
AFTER INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.check_person_source_mvd_grounding_fn();


CREATE OR REPLACE FUNCTION transducer.check_person_source_cfd_1_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT *
        FROM transducer._person_source AS R1,
        (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
        WHERE (R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R1.empid = R2.empid AND R1.hdate <> R2.hdate)
            OR (R2.empid IS NOT NULL AND R2.hdate IS NULL)
            OR (R2.empid IS NULL AND R2.hdate IS NOT NULL)) THEN
        RAISE EXCEPTION 'CFD violation on person_source: empid -> hdate %', NEW;
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER person_source_cfd_1_trigger
BEFORE INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.check_person_source_cfd_1_fn();


CREATE OR REPLACE FUNCTION transducer.check_person_source_cfd_2_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT *
        FROM transducer._person_source AS R1,
        (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
        WHERE (R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL AND R1.empid = R2.empid AND R1.dept <> R2.dept)
            OR (R2.empid IS NULL AND R2.dept IS NOT NULL)
            OR (R2.empid IS NULL AND R2.manager IS NOT NULL)
            OR (R2.empid IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NULL)
            OR (R2.empid IS NOT NULL AND R2.dept IS NULL AND R2.manager IS NOT NULL)) THEN
        RAISE EXCEPTION 'CFD violation on person_source: empid -> dept %', NEW;
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER person_source_cfd_2_trigger
BEFORE INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.check_person_source_cfd_2_fn();


CREATE OR REPLACE FUNCTION transducer.check_person_source_cfd_3_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT *
        FROM transducer._person_source AS R1,
        (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
        WHERE (R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL AND R1.dept = R2.dept AND R1.manager <> R2.manager)
            OR (R2.dept IS NOT NULL AND R2.manager IS NULL)
            OR (R2.dept IS NULL AND R2.manager IS NOT NULL)) THEN
        RAISE EXCEPTION 'CFD violation on person_source: dept -> manager %', NEW;
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER person_source_cfd_3_trigger
BEFORE INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.check_person_source_cfd_3_fn();


CREATE OR REPLACE FUNCTION transducer.check_person_source_inc_1_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF (NEW.manager IS NULL) THEN
        RETURN NEW;
    END IF;
    IF (NEW.manager = NEW.ssn) THEN
        RETURN NEW;
    END IF;
    IF EXISTS (SELECT DISTINCT NEW.manager
        FROM transducer._person_source
        EXCEPT (
            SELECT ssn AS manager
            FROM transducer._person_source
            UNION
            SELECT NEW.ssn AS manager
        )) THEN
        RAISE EXCEPTION 'INC violation: person_source.manager ⊆ person_source.ssn';
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER person_source_inc_1_trigger
BEFORE INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.check_person_source_inc_1_fn();


CREATE TABLE transducer._person_source_INSERT AS
SELECT * FROM transducer._person_source
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.source_person_source_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._person_source_INSERT VALUES(NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER source_person_source_INSERT_trigger
AFTER INSERT ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.source_person_source_INSERT_fn();


CREATE TABLE transducer._person_source_DELETE AS
SELECT * FROM transducer._person_source
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.source_person_source_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._person_source_DELETE VALUES(OLD.ssn, OLD.empid, OLD.name, OLD.hdate, OLD.phone, OLD.email, OLD.dept, OLD.manager);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER source_person_source_DELETE_trigger
AFTER DELETE ON transducer._person_source
FOR EACH ROW
EXECUTE FUNCTION transducer.source_person_source_DELETE_fn();


CREATE TABLE transducer._p_INSERT AS
SELECT * FROM transducer._p
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_p_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._p_INSERT VALUES(NEW.ssn, NEW.name);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_p_INSERT_trigger
AFTER INSERT ON transducer._p
FOR EACH ROW
EXECUTE FUNCTION transducer.target_p_INSERT_fn();


CREATE TABLE transducer._p_DELETE AS
SELECT * FROM transducer._p
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_p_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._p_DELETE VALUES(OLD.ssn, OLD.name);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_p_DELETE_trigger
AFTER DELETE ON transducer._p
FOR EACH ROW
EXECUTE FUNCTION transducer.target_p_DELETE_fn();


CREATE TABLE transducer._personphone_INSERT AS
SELECT * FROM transducer._personphone
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personphone_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._personphone_INSERT VALUES(NEW.ssn, NEW.phone);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_personphone_INSERT_trigger
AFTER INSERT ON transducer._personphone
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personphone_INSERT_fn();


CREATE TABLE transducer._personphone_DELETE AS
SELECT * FROM transducer._personphone
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personphone_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._personphone_DELETE VALUES(OLD.ssn, OLD.phone);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_personphone_DELETE_trigger
AFTER DELETE ON transducer._personphone
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personphone_DELETE_fn();


CREATE TABLE transducer._personemail_INSERT AS
SELECT * FROM transducer._personemail
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personemail_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._personemail_INSERT VALUES(NEW.ssn, NEW.email);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_personemail_INSERT_trigger
AFTER INSERT ON transducer._personemail
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personemail_INSERT_fn();


CREATE TABLE transducer._personemail_DELETE AS
SELECT * FROM transducer._personemail
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personemail_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._personemail_DELETE VALUES(OLD.ssn, OLD.email);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_personemail_DELETE_trigger
AFTER DELETE ON transducer._personemail
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personemail_DELETE_fn();


CREATE TABLE transducer._pe_INSERT AS
SELECT * FROM transducer._pe
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._pe_INSERT VALUES(NEW.ssn, NEW.empid);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_pe_INSERT_trigger
AFTER INSERT ON transducer._pe
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_INSERT_fn();


CREATE TABLE transducer._pe_DELETE AS
SELECT * FROM transducer._pe
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._pe_DELETE VALUES(OLD.ssn, OLD.empid);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_pe_DELETE_trigger
AFTER DELETE ON transducer._pe
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_DELETE_fn();


CREATE TABLE transducer._pe_hdate_INSERT AS
SELECT * FROM transducer._pe_hdate
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_hdate_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._pe_hdate_INSERT VALUES(NEW.empid, NEW.hdate);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_pe_hdate_INSERT_trigger
AFTER INSERT ON transducer._pe_hdate
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_hdate_INSERT_fn();


CREATE TABLE transducer._pe_hdate_DELETE AS
SELECT * FROM transducer._pe_hdate
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_hdate_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._pe_hdate_DELETE VALUES(OLD.empid, OLD.hdate);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_pe_hdate_DELETE_trigger
AFTER DELETE ON transducer._pe_hdate
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_hdate_DELETE_fn();


CREATE TABLE transducer._ped_INSERT AS
SELECT * FROM transducer._ped
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_ped_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._ped_INSERT VALUES(NEW.ssn, NEW.empid);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_ped_INSERT_trigger
AFTER INSERT ON transducer._ped
FOR EACH ROW
EXECUTE FUNCTION transducer.target_ped_INSERT_fn();


CREATE TABLE transducer._ped_DELETE AS
SELECT * FROM transducer._ped
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_ped_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._ped_DELETE VALUES(OLD.ssn, OLD.empid);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_ped_DELETE_trigger
AFTER DELETE ON transducer._ped
FOR EACH ROW
EXECUTE FUNCTION transducer.target_ped_DELETE_fn();


CREATE TABLE transducer._peddept_INSERT AS
SELECT * FROM transducer._peddept
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_peddept_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._peddept_INSERT VALUES(NEW.empid, NEW.dept);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_peddept_INSERT_trigger
AFTER INSERT ON transducer._peddept
FOR EACH ROW
EXECUTE FUNCTION transducer.target_peddept_INSERT_fn();


CREATE TABLE transducer._peddept_DELETE AS
SELECT * FROM transducer._peddept
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_peddept_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._peddept_DELETE VALUES(OLD.empid, OLD.dept);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_peddept_DELETE_trigger
AFTER DELETE ON transducer._peddept
FOR EACH ROW
EXECUTE FUNCTION transducer.target_peddept_DELETE_fn();


CREATE TABLE transducer._deptmanager_INSERT AS
SELECT * FROM transducer._deptmanager
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_deptmanager_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._deptmanager_INSERT VALUES(NEW.dept, NEW.manager);
        RETURN NEW;
    END IF;
END;
$$;


CREATE TRIGGER target_deptmanager_INSERT_trigger
AFTER INSERT ON transducer._deptmanager
FOR EACH ROW
EXECUTE FUNCTION transducer.target_deptmanager_INSERT_fn();


CREATE TABLE transducer._deptmanager_DELETE AS
SELECT * FROM transducer._deptmanager
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_deptmanager_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
        RETURN NULL;
    ELSE
        INSERT INTO transducer._deptmanager_DELETE VALUES(OLD.dept, OLD.manager);
        RETURN OLD;
    END IF;
END;
$$;


CREATE TRIGGER target_deptmanager_DELETE_trigger
AFTER DELETE ON transducer._deptmanager
FOR EACH ROW
EXECUTE FUNCTION transducer.target_deptmanager_DELETE_fn();


CREATE TABLE transducer._person_source_INSERT_JOIN AS
SELECT * FROM transducer._person_source
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.source_person_source_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._person_source_INSERT
    );

    INSERT INTO transducer._loop VALUES (1);
    INSERT INTO transducer._person_source_INSERT_JOIN (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER source_person_source_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_source_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_person_source_INSERT_JOIN_fn();


CREATE TABLE transducer._person_source_DELETE_JOIN AS
SELECT * FROM transducer._person_source
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.source_person_source_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._person_source_DELETE
    );

    INSERT INTO transducer._loop VALUES (1);
    INSERT INTO transducer._person_source_DELETE_JOIN (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER source_person_source_DELETE_JOIN_trigger
AFTER INSERT ON transducer._person_source_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_person_source_DELETE_JOIN_fn();


CREATE TABLE transducer._p_INSERT_JOIN AS
SELECT * FROM transducer._p
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_p_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._p_INSERT
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_p_INSERT_JOIN_trigger
AFTER INSERT ON transducer._p_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_p_INSERT_JOIN_fn();


CREATE TABLE transducer._p_DELETE_JOIN AS
SELECT * FROM transducer._p
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_p_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._p_DELETE
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_p_DELETE_JOIN_trigger
AFTER INSERT ON transducer._p_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_p_DELETE_JOIN_fn();


CREATE TABLE transducer._personphone_INSERT_JOIN AS
SELECT * FROM transducer._personphone
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personphone_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._personphone_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_personphone_INSERT_JOIN_trigger
AFTER INSERT ON transducer._personphone_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personphone_INSERT_JOIN_fn();


CREATE TABLE transducer._personphone_DELETE_JOIN AS
SELECT * FROM transducer._personphone
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personphone_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._personphone_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_personphone_DELETE_JOIN_trigger
AFTER INSERT ON transducer._personphone_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personphone_DELETE_JOIN_fn();


CREATE TABLE transducer._personemail_INSERT_JOIN AS
SELECT * FROM transducer._personemail
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personemail_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._personemail_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_personemail_INSERT_JOIN_trigger
AFTER INSERT ON transducer._personemail_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personemail_INSERT_JOIN_fn();


CREATE TABLE transducer._personemail_DELETE_JOIN AS
SELECT * FROM transducer._personemail
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_personemail_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._personemail_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_personemail_DELETE_JOIN_trigger
AFTER INSERT ON transducer._personemail_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_personemail_DELETE_JOIN_fn();


CREATE TABLE transducer._pe_INSERT_JOIN AS
SELECT * FROM transducer._pe
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._pe_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_pe_INSERT_JOIN_trigger
AFTER INSERT ON transducer._pe_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_INSERT_JOIN_fn();


CREATE TABLE transducer._pe_DELETE_JOIN AS
SELECT * FROM transducer._pe
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._pe_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_pe_DELETE_JOIN_trigger
AFTER INSERT ON transducer._pe_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_DELETE_JOIN_fn();


CREATE TABLE transducer._pe_hdate_INSERT_JOIN AS
SELECT * FROM transducer._pe_hdate
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_hdate_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._pe_hdate_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_pe_hdate_INSERT_JOIN_trigger
AFTER INSERT ON transducer._pe_hdate_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_hdate_INSERT_JOIN_fn();


CREATE TABLE transducer._pe_hdate_DELETE_JOIN AS
SELECT * FROM transducer._pe_hdate
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_pe_hdate_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._pe_hdate_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_pe_hdate_DELETE_JOIN_trigger
AFTER INSERT ON transducer._pe_hdate_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_pe_hdate_DELETE_JOIN_fn();


CREATE TABLE transducer._ped_INSERT_JOIN AS
SELECT * FROM transducer._ped
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_ped_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._ped_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_ped_INSERT_JOIN_trigger
AFTER INSERT ON transducer._ped_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_ped_INSERT_JOIN_fn();


CREATE TABLE transducer._ped_DELETE_JOIN AS
SELECT * FROM transducer._ped
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_ped_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._ped_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._peddept
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_ped_DELETE_JOIN_trigger
AFTER INSERT ON transducer._ped_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_ped_DELETE_JOIN_fn();


CREATE TABLE transducer._peddept_INSERT_JOIN AS
SELECT * FROM transducer._peddept
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_peddept_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._peddept_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_peddept_INSERT_JOIN_trigger
AFTER INSERT ON transducer._peddept_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_peddept_INSERT_JOIN_fn();


CREATE TABLE transducer._peddept_DELETE_JOIN AS
SELECT * FROM transducer._peddept
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_peddept_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._peddept_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._deptmanager
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_peddept_DELETE_JOIN_trigger
AFTER INSERT ON transducer._peddept_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_peddept_DELETE_JOIN_fn();


CREATE TABLE transducer._deptmanager_INSERT_JOIN AS
SELECT * FROM transducer._deptmanager
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_deptmanager_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._deptmanager_INSERT
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
    );

    INSERT INTO transducer._p_INSERT_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_INSERT_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_INSERT_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_INSERT_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_deptmanager_INSERT_JOIN_trigger
AFTER INSERT ON transducer._deptmanager_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_deptmanager_INSERT_JOIN_fn();


CREATE TABLE transducer._deptmanager_DELETE_JOIN AS
SELECT * FROM transducer._deptmanager
WHERE 1<>1;


CREATE OR REPLACE FUNCTION transducer.target_deptmanager_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    CREATE TEMPORARY TABLE temp_table(
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
        FROM transducer._deptmanager_DELETE
        NATURAL LEFT OUTER JOIN transducer._p
        NATURAL LEFT OUTER JOIN transducer._personphone
        NATURAL LEFT OUTER JOIN transducer._personemail
        NATURAL LEFT OUTER JOIN transducer._pe
        NATURAL LEFT OUTER JOIN transducer._pe_hdate
        NATURAL LEFT OUTER JOIN transducer._ped
        NATURAL LEFT OUTER JOIN transducer._peddept
    );

    INSERT INTO transducer._p_DELETE_JOIN (SELECT ssn, name FROM temp_table);
    INSERT INTO transducer._personphone_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
    INSERT INTO transducer._personemail_DELETE_JOIN (SELECT ssn, email FROM temp_table);
    INSERT INTO transducer._pe_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._pe_hdate_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
    INSERT INTO transducer._ped_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
    INSERT INTO transducer._peddept_DELETE_JOIN (SELECT empid, dept FROM temp_table);
    INSERT INTO transducer._loop VALUES (-1);
    INSERT INTO transducer._deptmanager_DELETE_JOIN (SELECT dept, manager FROM temp_table);

    DELETE FROM temp_table;
    DROP TABLE temp_table;
    RETURN NEW;
END;
$$;


CREATE TRIGGER target_deptmanager_DELETE_JOIN_trigger
AFTER INSERT ON transducer._deptmanager_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_deptmanager_DELETE_JOIN_fn();


CREATE OR REPLACE FUNCTION transducer.SOURCE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM transducer._loop,
        (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
        WHERE ABS(loop_start) = row_count.rc_value) THEN
        RETURN NULL;
    END IF;

    INSERT INTO transducer._p (SELECT DISTINCT ssn, name
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL)
        ON CONFLICT (ssn) DO NOTHING;
    INSERT INTO transducer._personphone (SELECT DISTINCT ssn, phone
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL)
        ON CONFLICT (ssn, phone) DO NOTHING;
    INSERT INTO transducer._personemail (SELECT DISTINCT ssn, email
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL)
        ON CONFLICT (ssn, email) DO NOTHING;
    IF EXISTS (SELECT * FROM transducer._person_source_INSERT_JOIN
              WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
    INSERT INTO transducer._pe (SELECT DISTINCT ssn, empid
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND empid IS NOT NULL)
        ON CONFLICT (empid) DO NOTHING;
    END IF;
    IF EXISTS (SELECT * FROM transducer._person_source_INSERT_JOIN
              WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
    INSERT INTO transducer._pe_hdate (SELECT DISTINCT empid, hdate
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND empid IS NOT NULL)
        ON CONFLICT (empid) DO NOTHING;
    END IF;
    IF EXISTS (SELECT * FROM transducer._person_source_INSERT_JOIN
              WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
    INSERT INTO transducer._ped (SELECT DISTINCT ssn, empid
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND empid IS NOT NULL)
        ON CONFLICT (empid) DO NOTHING;
    END IF;
    IF EXISTS (SELECT * FROM transducer._person_source_INSERT_JOIN
              WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
    INSERT INTO transducer._peddept (SELECT DISTINCT empid, dept
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND empid IS NOT NULL)
        ON CONFLICT (empid) DO NOTHING;
    END IF;
    IF EXISTS (SELECT * FROM transducer._person_source_INSERT_JOIN
              WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
    INSERT INTO transducer._deptmanager (SELECT DISTINCT dept, manager
        FROM transducer._person_source_INSERT_JOIN
        WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dept IS NOT NULL)
        ON CONFLICT (dept) DO NOTHING;
    END IF;

    DELETE FROM transducer._person_source_INSERT;
    DELETE FROM transducer._person_source_INSERT_JOIN;
    DELETE FROM transducer._loop;
    RETURN NEW;
END;
$$;


CREATE TRIGGER SOURCE_INSERT_FN_trigger_person_source
AFTER INSERT ON transducer._person_source_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.SOURCE_INSERT_FN();


CREATE OR REPLACE FUNCTION transducer.TARGET_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM transducer._loop,
        (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
        WHERE ABS(loop_start) = row_count.rc_value) THEN
        RETURN NULL;
    END IF;

    CREATE TEMPORARY TABLE temp_table_join(
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
        FROM transducer._p_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._personphone_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._personemail_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._pe_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._pe_hdate_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._ped_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._peddept_INSERT_JOIN
        NATURAL LEFT OUTER JOIN transducer._deptmanager_INSERT_JOIN
        WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND ((empid IS NULL AND hdate IS NULL AND dept IS NULL AND manager IS NULL) OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL AND manager IS NULL) OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL))
    );

    -- Tuple containment: keep only most informative tuples
    IF EXISTS (SELECT * FROM temp_table_join WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
        DELETE FROM temp_table_join t_poor
        WHERE t_poor.empid IS NULL AND t_poor.hdate IS NULL
        AND EXISTS (
            SELECT 1 FROM temp_table_join t_rich
            WHERE t_rich.ssn = t_poor.ssn AND t_rich.name = t_poor.name AND t_rich.phone = t_poor.phone AND t_rich.email = t_poor.email
            AND t_rich.empid IS NOT NULL AND t_rich.hdate IS NOT NULL
        );
    END IF;
    IF EXISTS (SELECT * FROM temp_table_join WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
        DELETE FROM temp_table_join t_poor
        WHERE t_poor.dept IS NULL AND t_poor.manager IS NULL
        AND EXISTS (
            SELECT 1 FROM temp_table_join t_rich
            WHERE t_rich.ssn = t_poor.ssn AND t_rich.name = t_poor.name AND t_rich.phone = t_poor.phone AND t_rich.email = t_poor.email
            AND t_rich.empid IS NOT NULL AND t_rich.hdate IS NOT NULL AND t_rich.dept IS NOT NULL AND t_rich.manager IS NOT NULL
        );
    END IF;

    INSERT INTO transducer._person_source (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table_join)
        ON CONFLICT (ssn, phone, email) DO NOTHING;
    INSERT INTO transducer._loop VALUES (-1);


    DELETE FROM transducer._p_INSERT;
    DELETE FROM transducer._p_INSERT_JOIN;
    DELETE FROM transducer._personphone_INSERT;
    DELETE FROM transducer._personphone_INSERT_JOIN;
    DELETE FROM transducer._personemail_INSERT;
    DELETE FROM transducer._personemail_INSERT_JOIN;
    DELETE FROM transducer._pe_INSERT;
    DELETE FROM transducer._pe_INSERT_JOIN;
    DELETE FROM transducer._pe_hdate_INSERT;
    DELETE FROM transducer._pe_hdate_INSERT_JOIN;
    DELETE FROM transducer._ped_INSERT;
    DELETE FROM transducer._ped_INSERT_JOIN;
    DELETE FROM transducer._peddept_INSERT;
    DELETE FROM transducer._peddept_INSERT_JOIN;
    DELETE FROM transducer._deptmanager_INSERT;
    DELETE FROM transducer._deptmanager_INSERT_JOIN;
    DELETE FROM transducer._loop;
    DELETE FROM temp_table_join;
    DROP TABLE temp_table_join;
    RETURN NEW;
END;
$$;


CREATE TRIGGER TARGET_INSERT_FN_trigger_p
AFTER INSERT ON transducer._p_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_personphone
AFTER INSERT ON transducer._personphone_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_personemail
AFTER INSERT ON transducer._personemail_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_pe
AFTER INSERT ON transducer._pe_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_pe_hdate
AFTER INSERT ON transducer._pe_hdate_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_ped
AFTER INSERT ON transducer._ped_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_peddept
AFTER INSERT ON transducer._peddept_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE TRIGGER TARGET_INSERT_FN_trigger_deptmanager
AFTER INSERT ON transducer._deptmanager_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_INSERT_FN();


CREATE OR REPLACE FUNCTION transducer.SOURCE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM transducer._loop,
        (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
        WHERE loop_start = row_count.rc_value) THEN
        RETURN NULL;
    END IF;

    IF EXISTS (SELECT * FROM transducer._person_source WHERE ssn = NEW.ssn
        EXCEPT (SELECT * FROM transducer._person_source WHERE ssn = NEW.ssn AND phone = NEW.phone)) THEN
        DELETE FROM transducer._personphone WHERE (ssn, phone) IN
            (SELECT ssn, phone FROM transducer._person_source_DELETE_JOIN
            );
    END IF;
    IF EXISTS (SELECT * FROM transducer._person_source WHERE ssn = NEW.ssn
        EXCEPT (SELECT * FROM transducer._person_source WHERE ssn = NEW.ssn AND email = NEW.email)) THEN
        DELETE FROM transducer._personemail WHERE (ssn, email) IN
            (SELECT ssn, email FROM transducer._person_source_DELETE_JOIN
            );
    END IF;

    IF NOT EXISTS (SELECT * FROM transducer._person_source WHERE ssn = NEW.ssn AND phone = NEW.phone AND email = NEW.email
        EXCEPT (SELECT * FROM transducer._person_source WHERE ssn = NEW.ssn AND phone = NEW.phone AND email = NEW.email AND empid = NEW.empid AND name = NEW.name AND hdate = NEW.hdate AND dept = NEW.dept AND manager = NEW.manager)) THEN
        DELETE FROM transducer._p WHERE ssn = NEW.ssn;
        DELETE FROM transducer._personphone WHERE ssn = NEW.ssn AND phone = NEW.phone;
        DELETE FROM transducer._personemail WHERE ssn = NEW.ssn AND email = NEW.email;
        DELETE FROM transducer._pe WHERE empid = NEW.empid;
        DELETE FROM transducer._pe_hdate WHERE empid = NEW.empid;
        DELETE FROM transducer._ped WHERE empid = NEW.empid;
        DELETE FROM transducer._peddept WHERE empid = NEW.empid;
        DELETE FROM transducer._deptmanager WHERE dept = NEW.dept;
    END IF;

    DELETE FROM transducer._person_source_DELETE;
    DELETE FROM transducer._person_source_DELETE_JOIN;
    DELETE FROM transducer._loop;
    RETURN NEW;
END;
$$;


CREATE TRIGGER SOURCE_DELETE_FN_trigger_person_source
AFTER INSERT ON transducer._person_source_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.SOURCE_DELETE_FN();


CREATE OR REPLACE FUNCTION transducer.TARGET_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM transducer._loop,
        (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
        WHERE ABS(loop_start) = row_count.rc_value) THEN
        RETURN NULL;
    END IF;

    CREATE TEMPORARY TABLE temp_table_join(
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
        FROM transducer._p_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._personphone_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._personemail_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._pe_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._pe_hdate_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._ped_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._peddept_DELETE_JOIN
        NATURAL LEFT OUTER JOIN transducer._deptmanager_DELETE_JOIN
        WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND ((empid IS NULL AND hdate IS NULL AND dept IS NULL AND manager IS NULL) OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL AND manager IS NULL) OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL))
    );

    DELETE FROM transducer._person_source WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);


    DELETE FROM transducer._p_DELETE;
    DELETE FROM transducer._p_DELETE_JOIN;
    DELETE FROM transducer._personphone_DELETE;
    DELETE FROM transducer._personphone_DELETE_JOIN;
    DELETE FROM transducer._personemail_DELETE;
    DELETE FROM transducer._personemail_DELETE_JOIN;
    DELETE FROM transducer._pe_DELETE;
    DELETE FROM transducer._pe_DELETE_JOIN;
    DELETE FROM transducer._pe_hdate_DELETE;
    DELETE FROM transducer._pe_hdate_DELETE_JOIN;
    DELETE FROM transducer._ped_DELETE;
    DELETE FROM transducer._ped_DELETE_JOIN;
    DELETE FROM transducer._peddept_DELETE;
    DELETE FROM transducer._peddept_DELETE_JOIN;
    DELETE FROM transducer._deptmanager_DELETE;
    DELETE FROM transducer._deptmanager_DELETE_JOIN;
    DELETE FROM transducer._loop;
    DELETE FROM temp_table_join;
    DROP TABLE temp_table_join;
    RETURN NEW;
END;
$$;


CREATE TRIGGER TARGET_DELETE_FN_trigger_p
AFTER INSERT ON transducer._p_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_personphone
AFTER INSERT ON transducer._personphone_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_personemail
AFTER INSERT ON transducer._personemail_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_pe
AFTER INSERT ON transducer._pe_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_pe_hdate
AFTER INSERT ON transducer._pe_hdate_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_ped
AFTER INSERT ON transducer._ped_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_peddept
AFTER INSERT ON transducer._peddept_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();


CREATE TRIGGER TARGET_DELETE_FN_trigger_deptmanager
AFTER INSERT ON transducer._deptmanager_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.TARGET_DELETE_FN();
