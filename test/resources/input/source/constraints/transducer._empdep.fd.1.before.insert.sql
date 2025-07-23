CREATE OR REPLACE FUNCTION transducer._empdep_fd_1_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._EMPDEP AS r1,
         (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.dep_name,NEW.dep_address) AS r2
            WHERE  r1.dep_name = r2.dep_name 
         AND r1.dep_address<> r2.dep_address) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN EMPDEP';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;