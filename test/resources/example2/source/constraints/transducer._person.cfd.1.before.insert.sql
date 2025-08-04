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
