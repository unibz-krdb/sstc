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