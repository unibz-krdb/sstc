CREATE TABLE transducer._PE_HDATE AS
	SELECT DISTINCT empid, hdate FROM transducer._PERSON
	WHERE empid IS NOT NULL AND hdate IS NOT NULL;
ALTER TABLE transducer._PE_HDATE ADD PRIMARY KEY (empid);
ALTER TABLE transducer._PE_HDATE ADD FOREIGN KEY (empid) REFERENCES transducer._PE(empid);