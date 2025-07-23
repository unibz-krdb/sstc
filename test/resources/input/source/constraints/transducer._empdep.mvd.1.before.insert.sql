CREATE OR REPLACE FUNCTION transducer.check_EMPDEP_mvd_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT DISTINCT r1.ssn, r2.name, r1.phone, r1.email, r2.dep_name, r2.dep_address 
         FROM transducer._EMPDEP AS r1,
         (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.dep_name, NEW.dep_address) AS r2
            WHERE  r1.ssn = r2.ssn 
         EXCEPT
         SELECT *
         FROM transducer._EMPDEP
         ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON PHONE %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;
