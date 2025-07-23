CREATE OR REPLACE FUNCTION transducer.check_EMPDEP_mvd_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS   (SELECT r1.ssn, r1.name, r1.phone, NEW.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.name, NEW.phone, r1.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._EMPDEP)) THEN
      RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', NEW;
      INSERT INTO transducer._EMPDEP 
            (SELECT r1.ssn, r1.name, r1.phone, NEW.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.name, NEW.phone, r1.email, r1.dep_name, r1.dep_address
            FROM transducer._EMPDEP as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._EMPDEP));
      RETURN NEW;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;