CREATE TABLE transducer._PED AS
	SELECT DISTINCT ssn, empid FROM transducer._PERSON
	WHERE empid IS NOT NULL AND dept IS NOT NULL;
ALTER TABLE transducer._PED ADD PRIMARY KEY (empid);
ALTER TABLE transducer._PED ADD FOREIGN KEY (empid) REFERENCES transducer._PE(empid);