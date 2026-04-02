/* TESTING AREA */

SELECT * FROM transducer._PERSON;

SELECT * FROM transducer._PERSON_INSERT;
SELECT * FROM transducer._PERSON_INSERT_JOIN;

SELECT * FROM transducer._P;
SELECT * FROM transducer._PE;
SELECT * FROM transducer._PE_HDATE;
SELECT * FROM transducer._PED;
SELECT * FROM transducer._PED_DEPT;
SELECT * FROM transducer._DEPT_MANAGER;
SELECT * FROM transducer._PERSON_PHONE;
SELECT * FROM transducer._PERSON_EMAIL;

SELECT * FROM transducer._P_INSERT;
SELECT * FROM transducer._PE_INSERT;
SELECT * FROM transducer._PE_HDATE_INSERT;
SELECT * FROM transducer._PED_INSERT;
SELECT * FROM transducer._PED_DEPT_INSERT;
SELECT * FROM transducer._DEPT_MANAGER_INSERT;
SELECT * FROM transducer._PERSON_PHONE_INSERT;
SELECT * FROM transducer._PERSON_EMAIL_INSERT;

SELECT DISTINCT * FROM transducer._P_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._PE_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._PE_HDATE_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._PED_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._PED_DEPT_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._DEPT_MANAGER_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._PERSON_PHONE_INSERT_JOIN;
SELECT DISTINCT * FROM transducer._PERSON_EMAIL_INSERT_JOIN;

SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL;


/* UPDATES */

INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jitte', 'hdate4', 'phone41', 'mail42', 'dep1', 'ssn1'); 
INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jitte', 'hdate4', 'phone41', 'mail42', 'dep2', 'ssn2');

INSERT INTO transducer._PED_DEPT VALUES ('emp4', 'dep1');

INSERT INTO transducer._PERSON_PHONE VALUES ('ssn1', 'phone12');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn1', 'mail12');

INSERT INTO transducer._PERSON_PHONE VALUES ('ssn2', 'phone22');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn2', 'mail22');

INSERT INTO transducer._PERSON_PHONE VALUES ('ssn3', 'phone32');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn3', 'mail32');	

BEGIN;
INSERT INTO transducer._loop VALUES (4);
INSERT INTO transducer._P VALUES ('ssn6', 'Jolly');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn6', 'phone61');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn6', 'mail61');
END;

BEGIN;
INSERT INTO transducer._loop VALUES (6);
INSERT INTO transducer._P VALUES ('ssn5', 'Jex');
INSERT INTO transducer._PE VALUES ('ssn5', 'emp5');
INSERT INTO transducer._PE_HDATE VALUES ('emp5', 'hdate5');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn5', 'phone51');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn5', 'mail51');
END;

BEGIN;
INSERT INTO transducer._loop VALUES (9);
INSERT INTO transducer._P VALUES ('ssn7', 'Jad');
INSERT INTO transducer._PE VALUES ('ssn7', 'emp7');
INSERT INTO transducer._PE_HDATE VALUES ('emp7', 'hdate7');
INSERT INTO transducer._PED VALUES ('ssn7', 'emp7');
INSERT INTO transducer._DEPT_MANAGER VALUES ('dep2', 'ssn7');
INSERT INTO transducer._PED_DEPT VALUES ('emp7', 'dep2');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn7', 'phone71');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn7', 'mail71');
END;



ROLLBACK;
