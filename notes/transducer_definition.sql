/*Transducer Architecture*/
/*
In this section we present the core of the transducer architecture and how we think the two layer solve the issue with multi-tables source and target schemas.
Let's call S and T the source and target database schema and S1, S2, ..., Sn, T1, T2, ..., Tm their respective tables. They can already exists in the initial state,
or one can be deduced from applying a set of mapping to the other. Both are expected to be equivalent, a property guarantee by a previous process, and so, every
possible instance allowed in one schema exists in the other, and inversely. The most interesting consequence of their assumed equivalence is the capacity for
bidirectional updates. 

To do so, we begin by developing a first layer, called the update layer, destined to hold the new added tuples in case of an INSERT, 
or the old removed tuples in case of a DELETE. We do so by adding for each table in both schema both an INSERT table noted SIi, and a DELETE table SDi, both 
holding updates done on a source table Si.

	   S1     	   S2   ... 	Sn 
	 /	  \ 		 /	  \		 /	  \   
   SI1  SD1	   SI2  SD2    SIn  SDn 

   TI1  TD1	   TI2  TD2    TIm  TDm
    \	  /		 \	  /		 \	   /
   	TI 		  T2	  ...    Tm

By extracting the updates tuples this way, we avoid comparison operations between S and T to discover the modification made. Here, each modification is directly
inserted into the update layer. We now present the functions dealing with this propagation, as well as the SQL triggers used, starting with INSERT. We unfold our
methodology starting from the source database S and its multiple tables, Si in S1, ..., Sn, but the same logic, notation and code also applies to the reverse
transactions and updates starting from T:
*/

CREATE OR REPLACE FUNCTION Si_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO SIi VALUES(NEW.X, NEW.Y, ... );
   RETURN NEW;
END IF;
END;  $$;

CREATE TRIGGER Si_INSERT_TRIGGER
AFTER INSERT ON Si
FOR EACH ROW
EXECUTE FUNCTION Si_INSERT_FN();

/*
And now for the DELETE
*/

CREATE OR REPLACE FUNCTION Si_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM loop WHERE loop_start = 1) THEN
	RETURN NULL;
ELSE
	INSERT INTO SDi VALUES(OLD.X, OLD.Y, ... );
	RETURN NEW;
END IF;
END;  $$;

CREATE TRIGGER Si_DELETE_TRIGGER
AFTER DELETE ON Si
FOR EACH ROW
EXECUTE FUNCTION Si_DELETE_FN();

/*
The loop aspect will be present further in the SQL TRICKS section, but to summarize its purpose it is to prevent INSERT and DELETE done on one
side of the transducer to loop back around. Each trigger-function couple presented here are the base of the transducer architecture and work as such:
Whenever an update is done over a database schema, the trigger associated to the modified tables activates and the NEW or OLD element gets inserted into
the proper update tables. After a transaction done on multiple table in S, for instance, each modified table will propagate said modification into the
set of associate update tables. And this is where it gets a bit more complicated. After an update done over S, the initial intuition would be to use these
updates tables as the new targets when building the mapping S -> T, something written below:
*/

INSERT INTO T1 (SELECT DISTINCT ATT1, ATT2, ... FROM SI1, SI2, ... WHERE ...);
...
INSERT INTO Tm (SELECT DISTINCT ATT1, ATT2, ... FROM SI1, SI2, ... WHERE ...);

/*
We ignore for the moment the timing of the triggers and assume that the mapping from the update tables is always done when each update tables is filled. In the
SQL TRICK section we specify the timing problematic and how we choose to solve it. For now, let's assume that a list of triggers updates tables INSERTs
triggers the mapping function shown below and that timing is not a thing:
*/

CREATE OR REPLACE FUNCTION SOURCE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
INSERT INTO T1 (SELECT DISTINCT ATT1, ATT2, ... FROM SI1, SI2, ... WHERE ...);
...
INSERT INTO Tm (SELECT DISTINCT ATT1, ATT2, ... FROM SI1, SI2, ... WHERE ...);
RETURN NEW;
END;  $$;

CREATE OR REPLACE TRIGGER SOURCE_INSERT_TRIGGER_1
AFTER INSERT ON SI1
FOR EACH ROW
EXECUTE FUNCTION SOURCE_INSERT_FN();

CREATE OR REPLACE TRIGGER SOURCE_INSERT_TRIGGER_2
AFTER INSERT ON SI2
FOR EACH ROW
EXECUTE FUNCTION SOURCE_INSERT_FN();

...

CREATE OR REPLACE TRIGGER SOURCE_INSERT_TRIGGER_n
AFTER INSERT ON SIn
FOR EACH ROW
EXECUTE FUNCTION SOURCE_INSERT_FN();


/*
A last aspect we ignore for now is the INSERT and DELETE orders, as well as the order in which NATURAL JOINS, when used, are done.
Here, the complexity comes from partial transactions, that is, transactions updates which do not include every tables of a database schema. For instance,
if in a table PERSON:(ssn, name, phone, department) then split into PERSON_Name:(ssn, name) and PERSON_Phone:(ssn, phone) we have the following MVD ssn ->> phone, 
then it should be possible for the same person to have multiple phone numbers. Which in turns means that a single INSERT {'ssn1','phone2'} into PERSON_Phone 
should be enough to recreate the tuple {'ssn1', 'John', 'phone12', 'dep1'} back into PERSON, assuming that PERSON_Name already contains {'ssn1', 'John'} 
and PERSON {'ssn1', 'John', 'phone11', 'dep1'}. In this case, it is neither possible, nor desirable, to create a transaction from the split schema containing an 
INSERT on both PERSON_Name and PERSON_Phone since no new tuples would actually be added into PERSON_Name. And so, the previous idea of only using values 
present in the update tables no longer works.

From this point, there are a few options at our disposal: A first one would be to rewrite the mapping function SOURCE_INSERT_FN taking into consideration every
possible scenarios. A decision tree checking for each possible case where SI1, SI2 are filled, SI1 is but not SI2 and so on and consequently writing a new
mapping function alternating between filled source update tables and original source table. A shorten version of this decision tree with the PERSON example is shown
as such, ignoring department for the moment:
*/

IF EXISTS (SELECT * FROM PERSON_PHONE_INSERT) THEN
	IF EXISTS (SELECT * FROM PERSON_NAME_INSERT) THEN
		INSERT INTO PERSON (SELECT ssn, name, phone FROM PERSON_PHONE_INSERT, PERSON_NAME_INSERT);
	ELSE
		INSERT INTO PERSON (SELECT ssn, name, phone FROM PERSON_PHONE_INSERT, PERSON_NAME);
	END IF;
	RETURN NEW;
END IF;
RETURN NULL;


/*
While relatively elegant in a trivial example, the complexity of the tree greatly increase as our database's table can be updated independently. MVDs are one
thing, but a similar situation also happen whenever we have class hierarchy in which a table is a specialization of another. Here, a hierarchy can be added to
this example by adding the table Employee:(ssn, department) and making department a nullable attribute. Two tuples {'ssn1', 'John', 'phone11', 'dep1'} and
{'ssn2', 'Jane', 'phone21', NULL} would be split into PERSON_Name {'ssn1', 'John'}, {'ssn2', 'Jane'}, PERSON_Phone {'ssn1', 'phone11'}, {'ssn2', 'phone21'} and
finally Employee {'ssn1', 'dep1'} as such. Here the case by case mapping executed by the decision became even more tricky as it require more than just modification
of the target tables:
*/

IF EXISTS (SELECT * FROM PERSON_PHONE_INSERT) THEN
	IF EXISTS (SELECT * FROM PERSON_NAME_INSERT) THEN
		IF EXISTS (SELECT * FROM Employee_INSERT) THEN
			...

/*
An alternative to the decision tree is to find some optimization along each possible cases. For instance, if only a phone number gets added there is no point
in testing further conditions regarding the other tables. But similarly to the tree approach, finely tuning each scenarios is a lot of work and require a keen
understanding of the mapping used. Once more, it would be desirable if there was a more generic approach to this problem, one which doesn't require to modulate
the main mapping while still allowing for partial updates. A first solution is too use a simplified mapping to recreate full tuple from partial updates. This can be
done quite seemingly easily with a broad NATURAL JOIN over PERSON_PHONE_INSERT and each of the other split tables as such:
*/

SELECT * FROM PERSON_PHONE_INSERT 
NATURAL JOIN PERSON_NAME
NATURAL JOIN Employee


/*
This solution however has two consequent restriction:
1. Each tables must be linked in some way via foreign keys
2. There are incorrect natural join orders which do not return the proper result

The first requirement states that both database schema must be well built and that a full tuple be composed. By full tuple we mean a tuple containing most, if not
all, attributes from a database schema. The example we use here fits as it is possible to compose a ssn,name,phone,department tuple from any of the split tables.
More generically, this requires a graph like structure where it is possible from any node to get information from any other node in the graph. We can draw a more
complex example as such:

								[T1]
								/  \
							[T2]  [T3] - [T4]
							/  \
						[T5]  [T6]
								 |
								[T7]
								/  \
							[T8]  [T9] - [T10] - [T11]

Let's now assume that we can update T11 independently from the rest of the table, in the other approaches we would had to write a complicated decision tree
checking for each INSERT states or some other complex operation. But here, we can just write:
*/

SELECT * FROM T11_INSERT 
NATURAL JOIN T10
NATURAL JOIN T9
...
NATURAL JOIN T1

/*
Neat.
Unfortunately, for this query to work the order of the natural join has to be correct. An intriguing result we got after some tests is that the natural join of
two tables not sharing any elements return their cartesian product. This of course lead in fine to a lot of unexpected and incorrect output. We don't have a
proper way of finding a correct natural join order yet, but I'm pretty sure that at least one exists for any connected databases. And before moving on, let's
look at another example:

								[T1]										[T12]										[T16]					
								/  \										  |
							[T2]  [T3] - [T4]							[T13]
							/  \										   /  \
						[T5]  [T6]								  [T14]  [T15]
								 |
								[T7]
								/  \
							[T8]  [T9] - [T10] - [T11]

What is our answer to this kind of database schema in which multiple networks exists at once independently from each other?
The full natural join of every table would be far to large to be useful, especially since every join with a foreign table lead to a product, multiplying
exponentially the results. That being said, a simple DISTNCT projection over every attributes present in T1-T11 would be enough to get the proper result.
But still, this can't be desirable. In this kind of situation, I believe an approach consisting of stating in advance the sets of tables and limiting the
join from any table of these group to remain internal. Basically, an update on T2 would join only the tables T1-T11, one on T13 only T12-15 and so on. Because 
we already require a curating of the join order, adding to this pre-process step a restriction on the tables selected doesn't seems out of place.

A last point regarding the full join query concerns the existence of null values. In our person example, department is nullable meaning that it has to be possible
to INSERT tuples in the split schema such that their join generate tuples which can also have no department. In other word, it should be possible to INSERT persons 
just as much as it should be possible to INSERT employees. Our proposal is the following query:
*/

SELECT * FROM PERSON_PHONE_INSERT 
NATURAL LEFT OUTER JOIN PERSON_NAME
NATURAL LEFT OUTER JOIN Employee

/*
And more generically, assuming the join order is correct
*/

SELECT * FROM S1
NATURAL LEFT OUTER JOIN S2
...
NATURAL LEFT OUTER JOIN Sn

/*
We call this query from each updated INSERT table. This may seem redundant, but we have a good example showing that it is necessary:
Let there be PERSON:(ssn, name, phone, email) with ssn,phone,email PK, ssn ->> phone and ssn ->> email. A consequence of the MVDs is that for each new phone added, 
an additional number of tuples satisfying the email MVD must also be added. In other words, given {ssn1, John, phone11, email11}, adding the tuple 
{ssn1, John, phone12, email12} requires the creation of two additional tuples {ssn1, John, phone11, email12} and {ssn1, John, phone12, email12}. This is an aspect 
we already explored when presenting the MVD constraint, so now we want to address this from an bidirectional update point-of-view. The creation of additional, 
implicit in a way, tuples make more sense when done from the other side of the mapping materialized by the schema T containing PERSON_Name(ssn, name), 
PERSON_Phone:(ssn, phone) and PERSON_Email:(ssn, email). Again, let's have each table populated as such:
PERSON_Name:{ssn1, John}
PERSON_Phone:{ssn1, phone11}
PERSON_Email:{ssn1, email11}

Let's now have the following transaction:
*/

BEGIN
INSERT INTO PERSON_PHONE VALUES (ssn1, phone12);
INSERT INTO PERSON_EMAIL VALUES (ssn1, email12);
END;

/*
Once {ssn1, phone12} gets added to PERSON_PHONE_INSERT, the NATURAL JOIN query triggers, returning the following tuple:
{{ssn1, John, phone12, email11}}
This makes sense as by that time the second INSERT has yet to be done. When it is, and once more when added to PERSON_EMAIL_INSERT, we get the following:
{{ssn1, John, phone11, email12},
{ssn1, John, phone12, email12}}

The union of both query results returns the full unfolding expected at the beginning of the example, thus justifying the use of the query in each INSERT tables.
Speaking off, how exactly does this union part work? Where goes the result of this query? And how does the source mapping ever match with a full tuple?
This is where we introduced the second layer, sometime called the prime layer in examples but I believe it is more fitting to call it the join layer. It is 
our answer to the existence of partial updates and it consist in another set of tables made from projections over SI1, SI2,..., SIn, SD1, ..., SDN,... as seen
below where each table in this new layer is noted with a J:

	   S1     	   S2   ... 	Sn 
	 /	  \ 		 /	  \		 /	  \   
   SI1  SD1	   SI2  SD2    SIn  SDn
	 |    |		 |    |		 |    |
   SIJ1  SDJ1	SIJ2  SDJ2  SIJn  SDJn

	TIJ1  TDJ1	TIJ2  TDJ2  TIJn  TDJn
	 |    |		 |    |		 |    |
   TI1  TD1	   TI2  TD2    TIm  TDm
    \	  /		 \	  /		 \	   /
   	TI 		  T2	  ...    Tm

More precisely, here is how this layer gets populated after an INSERT on SIi:


		S1     	   S2   ... 	SIi   ... 	Sn
		 \				 \				 |				/
 		  NATURAL JOIN QUERY STARTING FROM SIi
	    |				 |				 |			   |		-PROJECTIONS-
		SIJ1     	SIJ2   ... 	SIJi   ... 	SIJn


Which in SQL function language is written:
*/

CREATE OR REPLACE FUNCTION SIi_JOIN_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$

INSERT INTO SIJ1 (SELECT DISTINCT ATT1, ATT2 ... FROM SIi 
	NATURAL LEFT OUTER JOIN S1
	...
	NATURAL LEFT OUTER JOIN Sn);

INSERT INTO SIJ2 (SELECT DISTINCT ATT1, ATT2 ... FROM SIi 
	NATURAL LEFT OUTER JOIN S1
	...
	NATURAL LEFT OUTER JOIN Sn);

...

INSERT INTO SIJn (SELECT DISTINCT ATT1, ATT2 ... FROM SIi 
	NATURAL LEFT OUTER JOIN S1
	...
	NATURAL LEFT OUTER JOIN Sn);
RETURN NEW;
END;  $$;

/*
Surely there is a more convenient way of writing this function without having to compute the same NATURAL JOIN query n times. I don't know how to yet, but this 
component blatantly need some optimization. What this function do is basically compute the NATURAL JOIN query before projecting it to each tables in the join layer.
Once each INSERT has taken place over the first layer, and when there is only one INSERT left to be done on the join layer, we apply this concluding function:
*/

CREATE OR REPLACE FUNCTION SOURCE_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

INSERT INTO T1 (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);
INSERT INTO T2 (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);
...
INSERT INTO Tm (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);

DELETE FROM SI1;
DELETE FROM SI2;
...
DELETE FROM SIn;
DELETE FROM SIJ1;
...
DELETE FROM SIJN;
RETURN NEW;
END;  $$;

/*
Because of partial updates it was not always possible for every INSERT tables to be filled and so to be used for the original mappings, we created this second 
layer always ensuring that each of its table was never empty so it could perfectly get matched for each mappings. Naturally, this architecture also works for 
DELETES.

So far, we have presented updates in the transducer architecture only from the split schema, but what about updates coming from a schema holding the constraints we 
first described. Staying with our person example, let's now update from the table PERSON and see how it get added into the split tables of PERSON_NAME, 
PERSON_PHONE, and EMPLOYEE. We recall Person:(ssn, name, phone, department) with department nullable and ssn ->> phone. Starting from the function 
activated upon an INSERT on PERSON and the function triggered when PERSON_INSERT receive an INSERT. We also assume the existence of a set of mystery tables related 
to PERSON to show how constraints would still work in a many-to-many tables mappings:
*/

CREATE OR REPLACE FUNCTION PERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
INSERT INTO PERSON_INSERT VALUES(NEW.ssn, NEW.name, NEW.phone, NEW.department);
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION PERSON_JOIN_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

INSERT INTO PERSON_INSERT_JOIN (SELECT DISTINCT ssn, name, phone, department  FROM PERSON_INSERT 
	NATURAL LEFT OUTER JOIN ...
	...
	);

...

RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION SOURCE_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

INSERT INTO PERSON_NAME (SELECT ssn, name FROM PERSON_INSERT_JOIN) ON CONFLICT (ssn) DO NOTHING;
INSERT INTO PERSON_PHONE (SELECT ssn, phone FROM PERSON_INSERT_JOIN) ON CONFLICT (ssn,phone) DO NOTHING;
IF EXISTS (SELECT * FROM PERSON_INSERT_JOIN WHERE department IS NOT NULL) THEN
         INSERT INTO EMPLOYEE (SELECT ssn, department FROM PERSON_INSERT_JOIN WHERE special IS NOT NULL) ON CONFLICT (ssn) DO NOTHING;
END IF;

...

DELETE FROM PERSON_INSERT;
...

DELETE FROM PERSON_INSERT_JOIN;
...

RETURN NEW;
END;  $$;



/*
Okay, two things:
1. We didn't need to use the ON CONFLICT clause in our example before since 



*/



/*SQL TRICKS AND ADVANCED SOLUTIONS*/
/*
When describing the architecture we left aside several important aspect, such as the timing restriction and the INSERT / DELETE ordering. We can go over the second 
issue pretty quickly so let's start here. In our theory we have inclusion dependencies, written ind^=[R1,R2](ATT,ATT), making sure that each instance R1[ATT] is 
always equal and found in R2[ATT]. Returning to our person example, this constraint ind^=[PERSON_NAME, PERSON_PHONE](ssn,ssn) enforces that each person with a name 
must have at least one phone number and, inversely, each existing phone number must be related to at least one named person. However, this type of constraint can't 
really exist in SQL proper. And so for the simple reason that since it is not really possible to INSERT on two or more table at the same time, it would be 
impossible to actually update a database schema with containing this kind of bidirectional foreign keys. Generally, the fact that transaction in SQL have a special 
timing and each operation within takes place strictly after another is the source of plenty of issue we solved in this section.

Notoriously, the sequential aspect of transaction require an proper INSERT and DELETE order to exists not to violates any foreign constraints. In the person
example the order is quite simple and can look like that:
INSERT: PERSON_NAME, PERSON_PHONE, PERSON_EMAIL, EMPLOYEE
DELETE: EMPLOYEE, PERSON_EMAIL, PERSON_PHONE, PERSON_NAME

But it obviously can get a lot less trivial. For such case let's bring back a previous drawing:

								[T1]
								/  \
							[T2]  [T3] -- [T4]
							/  \
						[T5]  [T6]
								 |
								[T7]
								/  \
							[T8]  [T9] - [T10] -- [T11]

In this example, T4 and T11 are both decomposition from MVDs. In lieu of a proper ordering methodology, I would rather pinpoint some observation I had:
1. In database schema with a lot of sub tables depending on each other such as the example provided, an intuitive starting point is to begin INSERTs ordering 
from the top (T1 -> T2 -> T3 -> ... ), and inversely for the DELETEs, start from the bottom (T11, ...)
2. Tables made from MVD decomposition are usually composite key and so are always the ones depending on other tables, putting them at the end of an INSERT order
and at the beginning of a DELETE one.
3. In the T9, T10 subschema, we can deduce that the primary key of T10 is a foreign key of T9. The unfortunate consequence is that T9 is now dependent on T10,
meaning that any INSERT has to first apply over T10 and any DELETE has to start from T9.
Admittedly, the order required is less strict than the NATURAL JOIN one, and there is no aggravating consequences for the underlying values as an UPDATE
transaction with an incorrect order simply gets nullified.

Focusing on the transducer architecture itself, there is two main elements we have yet to explain:
1. How can we prevent recursive updates infinitly looping?
2. How do we wait for all but the last update to launch a function?

The first problematic occurs once an update fully traverse the two layers and modify the database schema on the other side. This modification is also an update, 
meaning that another parcour through the layer will occur, and so on and so on. And while in most case those looping updates won't actually have any repercussions 
on the actual data since the concerned values were either already inserted or deleted, the entire transaction or update operation will be nullified nonetheless
for violating existing constraints making any updates impossible. The solution we found was to create a control table called LOOP with a single attribute loop. 
It works as follows:

There are four main types of update table in the transduce: INSERT from source, INSERT from target, DELETE from source and DELETE from target. Each of those block 
must accomodate the INSERT or DELETE of multiple tuples accross multiple tables. Which means that starting a transaction with an empty loop table is not 
recommanded as it would only allow the first update of a transaction to occur. Our solution is to play around with specific values. For instance, an INSERT over 
the source also INSERT a '1' in the loop table. This operation is repeated for each operation in the transaction. On the other side, there is a condition for 
INSERT in the target stating that no '1' must exists in the loop table. From this side, the UPDATES add a '-1' to the loop table instead and it is the UPDATES from 
the source that stopped upon seing a '-1'.
Written into SQL functions, this gives the following:
*/

CREATE OR REPLACE FUNCTION Si_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM LOOP WHERE loop = -1) THEN
   DELETE FROM LOOP;
   RETURN NULL;
ELSE
   INSERT INTO LOOP VALUES(1);
   INSERT INTO SIi VALUES(...);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION Tj_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM LOOP WHERE loop = 1) THEN
   DELETE FROM LOOP;
   RETURN NULL;
ELSE
   INSERT INTO LOOP VALUES(-1);
   INSERT INTO TIj VALUES(...);
   RETURN NEW;
END IF;
END;  $$;

/*
To prevent clogging up the LOOP table, it gets cleaned up after each final function:
*/

CREATE OR REPLACE FUNCTION SOURCE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

INSERT INTO T1 (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);
INSERT INTO T2 (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);
...
INSERT INTO Tm (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);

DELETE FROM SI1;
DELETE FROM SI2;
...
DELETE FROM SIn;
DELETE FROM SIJ1;
...
DELETE FROM SIJN;

DELETE FROM LOOP;

RETURN NEW;
END;  $$;

