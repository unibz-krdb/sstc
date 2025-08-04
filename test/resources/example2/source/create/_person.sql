CREATE TABLE transducer._PERSON
    (
    	ssn VARCHAR(100) NOT NULL,
    	empid VARCHAR(100),
    	name VARCHAR(100) NOT NULL,
    	hdate VARCHAR(100),
    	phone VARCHAR(100) NOT NULL,
    	email VARCHAR(100) NOT NULL,
    	dept VARCHAR(100),
    	manager VARCHAR(100),
        PRIMARY KEY (ssn,phone,email)
    );
