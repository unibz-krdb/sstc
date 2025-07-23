CREATE OR REPLACE FUNCTION transducer.check_POSITION_FD_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._POSITION AS r1,
         (SELECT NEW.dep_address, NEW.city, NEW.country ) AS r2
            WHERE  r1.city = r2.city 
         AND r1.country<> r2.country) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN POSITION';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;