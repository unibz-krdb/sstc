CREATE TABLE transducer._PE AS
	SELECT DISTINCT ssn, empid FROM transducer._PERSON
	WHERE empid IS NOT NULL AND hdate IS NOT NULL;
ALTER TABLE transducer._PE ADD PRIMARY KEY (empid);
ALTER TABLE transducer._PE ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);