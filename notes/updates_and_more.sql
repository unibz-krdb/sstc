/*
This document pertain to one of the trickiest aspect of the transducer: the independence of updates.
But more precisely, this concerns the DELETE of only a subset of tuples given an universal join as a start.

For example, here are some potential tuples populating _EMPDEP and _POSITION:

EMDPEP:																POSITION:
{																	{
	{ssn1, John, phone11, email11, dep1, depadd1},						{depadd1, Paris, France},
	{ssn1, John, phone12, email11, dep1, depadd1},						{depadd2, London, UK},
	{ssn2, June, phone21, email21, dep2, depadd2},						...
	{ssn3, Joel, phone31, email31, dep1, depadd1},					}
	...
}

Those values also populates the decompose schema as such


PERSON:								PERSON_PHONE:							PERSON_EMAIL:	
{									{										{
	{ssn1, John, dep1},					{ssn1, phone11},						{ssn1, email11},
	{ssn2, June, dep2},					{ssn1, phone12},						{ssn2, email21},
	{ssn3, Joel, dep1},                 {ssn2, phone21},						{ssn3, email31},
	...									{ssn3, phone31},						...
}										...									}
									}

DEPARTMENT:							DEPARTMENT_CITY:						CITY_COUNTRY:
{									{										{
	{dep1, depadd1},					{depadd1, Paris},						{Paris, France},
	{dep2, depadd2},					{depadd2, London},						{London, UK},
	...									...										...
}									}										}


The issue of independent deletes can be seen in the delete of {ssn3, Joel, dep1} from PERSON. Why?
Because ss1 and ssn3 shares a department name, department address and so on, it would be a mistake to remove said department along {ssn3, Joel, dep1} as it would lead to 
undesirable DELETEs.
The base query look like this
*/

BEGIN;
INSERT INTO transducer._loop VALUES ('4');
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn3';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn3';
DELETE FROM transducer._PERSON WHERE ssn = 'ssn3';
END;

/*
The universal join table before splitting basically contains the following: {ssn3, Joel, phone31, email31, dep1, depadd1, Paris, France}.
Given this, how do we make sure to only remove from the table EMPDEP?
In the transducer I wrote:
*/

IF EXISTS (SELECT r1.ssn, r1.name, r1.phone, r1.email, r1.dep_name, r1.dep_address, r1.city, r1.country  
   FROM (transducer._POSITION NATURAL LEFT OUTER JOIN transducer._EMPDEP) AS r1, temp_table_join
   WHERE r1.dep_address = temp_table_join.dep_address AND r1.city = temp_table_join.city AND r1.country = temp_table_join.country
   EXCEPT SELECT * FROM temp_table_join) THEN
      DELETE FROM transducer._EMPDEP WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);
ELSE
      DELETE FROM transducer._EMPDEP WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);
      DELETE FROM transducer._POSITION WHERE (dep_address) IN (SELECT dep_address FROM temp_table_join);
END IF;

/*
Here, the condition recreate an universal join tuple coming from the source and constraint by the values present in the temporary table:
*/

SELECT r1.ssn, r1.name, r1.phone, r1.email, r1.dep_name, r1.dep_address, r1.city, r1.country  
FROM (transducer._POSITION NATURAL LEFT OUTER JOIN transducer._EMPDEP) AS r1, temp_table_join
WHERE r1.dep_address = temp_table_join.dep_address AND r1.city = temp_table_join.city AND r1.country = temp_table_join.country

/*
With values, this query returns something like 
{
	{ssn1, John, phone11, email11, dep1, depadd1, Paris, France},
	{ssn1, John, phone12, email11, dep1, depadd1, Paris, France},
	{ssn3, Joel, phone31, email31, dep1, depadd1, Paris, France},
	...
}

From this result, we remove the universal join we stored in the temporary table, so
{ssn3, Joel, phone31, email31, dep1, depadd1, Paris, France}

If this query brings result, and it does in this case, then the tuple to be removed is independent from POSITION, and we only delete the proper row in EMPDEP.
Otherwise, if for instance we DELETED {ssn2,... } instead, then there is no "independence" for a lack of better definition and we also delete {depadd2, London, UK} 
from POSITION.

This is tricky to generalize, I recognize, but maybe it could be done as such:
Let there be S1, S2, ..., Sn source tables and T1, T2, ..., Tm target tables. Putting ourselves in the source delete function, an idea could be to test for independence over 
every target tables. Let's now assume a temporary table NEW_temp containing the list of tuples deleted from the source and ALL_temp the list of every tuples from the source.
Both have the universal join relation signature and by definition NEW_temp is always subset-equal to ALL_temp. For each tables in target, we check if the set difference of 
ALL_temp by NEW_temp returns a result. Unconstrained it always will, which is why we need to figure out a condition restricting ATT_temp.
In the example we are familiar with now, it could be something like:
*/

IF EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn 
           EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn AND phone = NEW.phone)) THEN
   DELETE FROM transducer._PERSON_PHONE WHERE ssn = NEW.ssn AND phone = NEW.phone;
END IF;

/*
Which, if reworked to include our temporary table, would be
*/

IF EXISTS (SELECT * FROM ALL_temp WHERE ssn = NEW_temp.ssn 
           EXCEPT (SELECT * FROM NEW_temp)) THEN
   DELETE FROM transducer._PERSON_PHONE WHERE ssn = NEW.ssn AND phone = NEW.phone;
END IF;

/*Or, with transducer_NEW corresponding to NEW_temp*/

SELECT r1.ssn, r1.name, r1.phone, r1.email, r1.dep_name, r1.dep_address, r1.city, r1.country  
FROM (transducer._EMPDEP NATURAL JOIN transducer._POSITION) as r1, transducer._ALL as r2 
WHERE r1.ssn = r2.ssn
EXCEPT
SELECT * FROM transducer._ALL

/*
This query basically allow us to know if the result can be removed independently from the target schema, that is only from a couple of tables and not from every 
single one. But it doesn't inform us on much more than that, and it doesn't tell us which target table can be considered independent in this example. Maybe we check for each 
sets of attributes present in target tables. For instance, in the PERSON_PHONE example, we constrain it as such:
*/

SELECT * 
FROM ALL_temp 
WHERE name = NEW_temp.name AND email = NEW_temp.email AND dep_name = NEW_temp.dep_name
AND dep_address = NEW_temp.dep_address AND city = NEW_temp.city AND country = NEW_temp.city
EXCEPT (SELECT * FROM NEW_temp)
   
/*
In this case, the result is not empty, meaning that the tuple can be removed from PERSON_PHONE independently. And so, for any target table Ti containing attributes 
between ATTi and ATTj, we get:
*/

IF EXISTS (SELECT * FROM ALL_temp WHERE ATT1 = NEW_temp.ATT1 AND ... AND ATTi-1 = NEW_temp.ATTi-1 AND ATTj+1 = NEW_temp.ATTj+1 AND ...
           EXCEPT (SELECT * FROM NEW_temp)) THEN
   DELETE FROM Ti WHERE ATTi = NEW_temp.ATTi AND ATTi+1 = NEW_temp.ATTi+1 AND ... AND ATTj = NEW_temp.ATTj;
END IF;

/*
Further work and testing is probably required, but this makes a little of sense to me.



Another thing I forgot was to provide a couples of actual working updates. I added them here in addition with a bunch of SELECTS to verify the results:
*/
BEGIN;
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn3' AND phone = 'phone32';
END;

BEGIN;
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn2', 'phone22');
END;

BEGIN;
INSERT INTO transducer._EMPDEP VALUES ('ssn4', 'Jovial', 'phone41', 'mail41', 'dep2', 'depadd2');
END;

BEGIN;
INSERT INTO transducer._loop VALUES (4);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn4';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn4';
DELETE FROM transducer._PERSON WHERE ssn = 'ssn4';
END;

BEGIN;
INSERT INTO transducer._loop VALUES (7);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn1';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn1';
DELETE FROM transducer._PERSON WHERE ssn = 'ssn1';
DELETE FROM transducer._DEPARTMENT WHERE dep_name = 'dep1';
DELETE FROM transducer._DEPARTMENT_CITY WHERE dep_address = 'depadd1';
DELETE FROM transducer._CITY_COUNTRY WHERE city = 'Paris';
END;

SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._EMPDEP
NATURAL LEFT OUTER JOIN transducer._POSITION

SELECT ssn, name, phone, email, dep_name, dep_address, city, country
FROM transducer._CITY_COUNTRY
NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
NATURAL LEFT OUTER JOIN transducer._PERSON
NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL

SELECT * FROM transducer._loop

SELECT * FROM transducer._CITY_COUNTRY
SELECT * FROM transducer._DEPARTMENT_CITY
SELECT * FROM transducer._DEPARTMENT
SELECT * FROM transducer._PERSON
SELECT * FROM transducer._PERSON_PHONE
SELECT * FROM transducer._PERSON_EMAIL

SELECT * FROM transducer._CITY_COUNTRY_DELETE
SELECT * FROM transducer._DEPARTMENT_CITY_DELETE
SELECT * FROM transducer._DEPARTMENT_DELETE
SELECT * FROM transducer._PERSON_DELETE
SELECT * FROM transducer._PERSON_PHONE_DELETE
SELECT * FROM transducer._PERSON_EMAIL_DELETE

SELECT * FROM transducer._CITY_COUNTRY_DELETE_JOIN
SELECT * FROM transducer._DEPARTMENT_CITY_DELETE_JOIN
SELECT * FROM transducer._DEPARTMENT_DELETE_JOIN
SELECT * FROM transducer._PERSON_DELETE_JOIN
SELECT * FROM transducer._PERSON_PHONE_DELETE_JOIN
SELECT * FROM transducer._PERSON_EMAIL_DELETE_JOIN

SELECT * FROM transducer._EMPDEP
SELECT * FROM transducer._POSITION

SELECT * FROM transducer._EMPDEP_INSERT
SELECT * FROM transducer._POSITION_INSERT

SELECT * FROM transducer._EMPDEP_INSERT_JOIN
SELECT * FROM transducer._POSITION_INSERT_JOIN

SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country 
FROM transducer._EMPDEP_INSERT_JOIN
NATURAL JOIN transducer._POSITION_INSERT_JOIN

SELECT * FROM transducer._EMPDEP_DELETE
SELECT * FROM transducer._POSITION_DELETE

SELECT * FROM transducer._EMPDEP_DELETE_JOIN
SELECT * FROM transducer._POSITION_DELETE_JOIN

SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country 
FROM transducer._EMPDEP_DELETE_JOIN
NATURAL JOIN transducer._POSITION_DELETE_JOIN