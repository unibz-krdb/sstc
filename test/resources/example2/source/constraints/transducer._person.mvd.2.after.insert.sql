CREATE OR REPLACE FUNCTION transducer.check_PERSON_mvd_fn_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
	IF EXISTS 	(SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
				FROM transducer._PERSON as r1
				WHERE r1.ssn = NEW.ssn
				UNION
				SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
				FROM transducer._PERSON as r1
				WHERE r1.ssn = NEW.ssn
				EXCEPT 
				(SELECT * FROM transducer._PERSON)) THEN
		RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', NEW;
		INSERT INTO transducer._PERSON 
				(SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
				FROM transducer._PERSON as r1
				WHERE r1.ssn = NEW.ssn
				UNION
				SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
				FROM transducer._PERSON as r1
				WHERE r1.ssn = NEW.ssn
				EXCEPT 
				(SELECT * FROM transducer._PERSON));
		RETURN NEW;
	ELSE
		RETURN NEW;
	END IF;
END;
$$;