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