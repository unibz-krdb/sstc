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



