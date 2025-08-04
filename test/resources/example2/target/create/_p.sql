CREATE TABLE transducer._P AS 
	SELECT DISTINCT ssn, name FROM transducer._PERSON;
ALTER TABLE transducer._P ADD PRIMARY KEY (ssn);