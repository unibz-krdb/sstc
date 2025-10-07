 /*CONSTRAINTS*/

 /*
 This document serves as base understanding for writing complex database constraint use in literature into SQL notation. The prime example would be functional dependencies, 
 a type of constraint widely used accross several fields, from database normalization to information preservation passing by data profilling, but which do not have proper
 in language or in software implementations. And yet, as we aim to show here, it is possible to represent such constraint using only SQL functions and triggers, practically 
 checking pre-INSERT if a new tuple, or set of tuples, violates the integrity constraints.

 And so, the facsimile of constraint we present always start from the same base trigger over a table R (unless said otherwise):
 */

CREATE OR REPLACE TRIGGER R_FD_1
BEFORE INSERT ON R
FOR EACH ROW
EXECUTE FUNCTION CHECK_R_FD_1_FN();

/*
With the structure of this function being:
*/

REATE OR REPLACE FUNCTION CHECK_R_FD_1_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF (/*FD TEST*/) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN R';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

/* 
With RETURN NULL acting as a fail state in case of constraint violation. THe idea being that a single query, or at least a single decision tree, is enough to practically 
simulate a given constraint. We now present each of those constraint queries
*/

/*Functional Dependency (FD)*/
/*
Given a table R:(X,Y,Z), a primary key X and a functional dependency Y -> Z denoting that for any two tuples t1, t2, if t1[Y] = t2[Y], then t1[Z] = t2[Z].
Translating it in SQL can be done by checking for tuples violating this FD constraint.
Assuming that R contains {x1,y1,z1}, {x2,y2,z2}n {x3, y2,z2}, how will the following conditions react to an INSERT of
1. {x4, y1, z1}
2. {x5, y1, z2}
*/

IF EXISTS (SELECT * FROM R AS R1,
		  (SELECT NEW.X, NEW.Y, NEW.Z) AS R2
		  WHERE  R1.Y = R2.Y 
		  AND R1.Z<> R2.Z) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN R';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

/*
The condition query returns nothing in the first case, but will do so in the second, rightfully capturing an incorrect INSERT violating the FD.
This relatively simple query is build as follows:
*/

SELECT * FROM R1, R2
		  WHERE  R1.FD[LHS] = R2.FD[LHS] 
		  AND R1.FD[RHS]<> R2.FD[RHS]

/*
Here, LHS and RHS stands for Left-Hand Side and Right-Hand Side respectively. In trigger functions related to INSERTS or DELETES, the added or DELETEd tuples,
denoted in SQL by NEW and OLD, must be written in full in the FROM section. We have done so in the first example, but for the remaining ones we instead choose to
consider NEW/OLD as tables on their own and materialized into R2. This remove a bit of the fluff require in practice otherwise.

This is the simplest declination of this constraint, but what about more complex examples?
We return to the idea of overlapping dependencies used in our work and test if multiple of this functions still works in R:(X,Y,Z,T) if:
1. Y -> T, Z -> T
2. Y -> Z, Z -> T
3. YZ -> T, T-> Z

The testing of each case is basic as it only require each dependency to be translated into its own constraint function, and then test with various INSERT clauses.
We always starts with R containing the tuple {x1, y1, z1, t1} and {x2, y2, z2, t2}. We also ignore inputs such as {x3, y3, z3, t3} as they are always correct and
don't challenges the constraint functions.
Starting the first case:
*/

IF EXISTS (SELECT * FROM R1, R2
		  WHERE  R1.Y = R2.Y 
		  AND R1.T<> R2.T) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD1 CONSTRAINT';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

IF EXISTS (SELECT * FROM R1, R2
		  WHERE  R1.Z = R2.Z 
		  AND R1.T<> R2.T) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD2 CONSTRAINT';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

/*
This basically allows tuples such as {x3, y2, z2, t2} or {x4, y1, z1, t1} to be added properly, while preventing other such as {x5, y1, z1, t2}, {x6, y2, z1, t1} from getting
INSERTed.

Moving on to the second scenario:
*/

IF EXISTS (SELECT * FROM R1, R2
		  WHERE  R1.Y = R2.Y 
		  AND R1.Z<> R2.Z) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD1 CONSTRAINT';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

IF EXISTS (SELECT * FROM R1, R2
		  WHERE  R1.Z = R2.Z 
		  AND R1.T<> R2.T) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD2 CONSTRAINT';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

/*
There is some overlap regarding which tuples are allowed or not between this scenario and the last one, so let's move to more interesting INSERTs assuming {x3, y3, z3, t1}.
This allows tuples such as {x4, y3, z1, t1} and so on. 

And finally:
*/

IF EXISTS (SELECT * FROM R1, R2
		  WHERE  R1.Y = R2.Y
		  AND R1.Z = R2.Z
		  AND R1.T<> R2.T) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD1 CONSTRAINT';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

IF EXISTS (SELECT * FROM R1, R2
		  WHERE  R1.T = R2.T 
		  AND R1.Z<> R2.Z) THEN
	RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD2 CONSTRAINT';
	RETURN NULL;
ELSE
	RETURN NEW;
END IF;

/*
As annoying as this constraint is to resolve, it seemingly poses no problem to implement as it successfully prevent any tuple where YZT are interdependent, meaning that only 
tuples like {x3, y1, z1, t1} and so one are allowed.
*/


/*MultiValue Dependency (MVD)*/
/*
Given a table R:(X,Y,Z), a MVD X ->> Y states that for any four tuples t1, t2, t3, t4 where t1[X] = t2[X] = t3[X] = t4[X], the following applies:
t1[Y] = t3[Y]
t2[Y] = t4[Y]
t1[R\XY] = t4[R\XY]
t2[R\XY] = t3[R\XY]
The intuitive interpretation is that the couple Y,Z is somewhat independent with regard to the rest of the tuple. In practice, this means that an X value x1
can have any number of associated Y, as long as the rest of the attributes, here Z, remains consistent. Real world examples correspond to an worker having multiple phone 
number or multiple email address, each combination clogging up the table containing the MVD as such:
{{x1, y1, z1},
{x1, y2, z1},
{x1, y3, z1},...}

We note that even if in this example we wanted X to be the primary key and correspond to a ssn or some worker id, in the table it cannot and require to instead impose a 
composite primary key XY. Later on, we see that this composite key grow with the number of MVDs existing simultaneously.
Regarding the actual check for MVD, it is a bit tricky, but here is the basic query:
*/

IF EXISTS (SELECT DISTINCT R1.X, R1.Y, R2.Z
		   FROM R1, R2
		   WHERE  R1.X = R2.X
			EXCEPT
			SELECT * FROM R1
			) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON R';
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;

/*
Assuming a initial tuple {x1, y1, z1}, what this query does is basically prevent INSERTs which allow either new X {x2, y2, z2} or any number of Y for a X as long as Z is not
change as materialized in {x1, y2, z1}. Consequentially, {x1, y3, z3} would not work, despite not violating the composite KEY XY.

Where MVDs become a bit tricky is when multiple of them exists at the same time in a singular table. 
Extending the previous table to R:(X,Y,Z,T) with X- >> Y and X ->> Z, let's see what changes with the addition of a new MVD:
First off, the PRIMARY KEY changes from XY to XYZ. Then, we already face an issue regarding the query as the expected following pair do not actually work:
*/

SELECT DISTINCT R1.X, R1.Y, R2.Z, R2.T
FROM R1, R2
WHERE  R1.X = R2.X 
EXCEPT
SELECT *	FROM R1

SELECT DISTINCT R1.X, R2.Y, R1.Z, R2.T
FROM R1, R2
WHERE  R1.X = R2.X
EXCEPT
SELECT *	FROM R1

/*
Here, the crux of the problem lies in the fact that Z and Y in the first and second query respectively cannot take a new different values. For instance, once again 
assuming {x1, y1, z1, t1}, the tuple {x1, y2, z1, t1} will pass the first query with no problem, but fail the second every times. And the inverse is true for
{x1, y1, z2, t1}, whereas both tuple should be valid and can exist at the same time. This also applies to more complex sets of MVD like X ->> Z, XY ->> T, not even talking 
about any potential overlaps. As it stands, I don't really have a solution for the more complex scenarios, but I do have one for the example at hand in which MVDs share the 
same LHS!
*/

IF EXISTS (SELECT DISTINCT R1.X, R1.Y, R1.Z, R2.T
		   FROM R1, R2
		   WHERE  R1.X = R2.X
			EXCEPT
			SELECT * FROM R1
			) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON R';
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;

/*
This basically allows any variations of Y and Z in R, as long as X and T are fixed, allowing {x1, y2, z2, t1}, {x1, y3, z1, t1} and so, but rejecting {x1, x2, z3, t2},...
There is however one more important aspect of MVDs we have to tackle. The constraint we use is rather lenient, and so there is one aspect lost in the process. That aspect 
I would say is the completeness of the table. To understand, we need to return to the definition of MVDs, and especially at this part:
t1[Y] = t3[Y]
t2[Y] = t4[Y]
t1[R\XY] = t4[R\XY]
t2[R\XY] = t3[R\XY]
What this part entails is that for any pair XY, there must exists that many XZT. In practice, this means that {x1, y2, z2, t1}, adding two new pairs {x1, y2} and {x1, z2}, 
and so, the two tuples {x1, y1, z2, t1} and {x1, y2, z1, t1} must also exists as well. This become obvious when seen from the decomposed schema as a full join of
RXT:(X, T), RXY:(X, Y) and RXZ:(X, Z) in which we have {{x1, y1}}, {{x1, y1}, {x1, y2}} and {{x1, z1}, {x1, z2}} returns the last four tuples. We propose the following 
function which, for once AFTER the INSERT of a new tuple automatically ground all pairs:
*/

CREATE OR REPLACE FUNCTION CHECK_R1_A_MVD_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
	IF EXISTS 	(SELECT R1.X, R1.Y, R2.Z, R1.T
				FROM R1, R2
				WHERE R1.X = R2.X
				UNION
				SELECT R1.X, R2.Y, R1.Z, R1.T
				FROM R1, R2
				WHERE R1.X = R2.X
				EXCEPT 
				(SELECT * FROM R1)) THEN
		RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', R2; /*It make more sense with NEW*/
		INSERT INTO transducer._EMPDEP 
				(/*Literally the same query*/);
		RETURN NEW;
	ELSE
		RETURN NEW;
	END IF;
END;
$$;

/*
Once more, this function works well in this scenario but I cannot tell how well it would in a more complex one. We can show how these two functions scales up given a 
set of MVDs of the type X ->> Y1, X ->> Y2, ..., X->> Yn in a relation R:(X, Y1, Y2, ..., Yn, Z):
*/

SELECT DISTINCT R1.X, R1.Y1, R1.Y2, ..., R1.Yn, R2.Z
FROM R1, R2
WHERE  R1.X = R2.X
EXCEPT
SELECT * FROM R1

SELECT R1.X, R2.Y1, R1.Y2, ..., R1.Yn, R1.Z
FROM R1, R2
WHERE R1.X = R2.X
UNION
SELECT R1.X, R1.Y1, R2.Y2, ..., R1.Yn, R1.Z
FROM R1, R2
WHERE R1.X = R2.X
UNION
...
UNION
SELECT R1.X, R1.Y1, R1.Y2, ..., R2.Yn, R1.Z
FROM R1, R2
WHERE R1.X = R2.X
EXCEPT 
(SELECT * FROM R1)

/*
The existence of this type of constraint makes the needs for decompositions ever more prescient.

*/

/*Guard Dependencies*/
/*
Or constraint based on specific values. One would expect a constraint based on real values to be more intuitive to translate into SQL, but it's not that simple.
In fact, a guard dependency g(cond) R != NULL denoting the potential presence of tuples satisfying the condition cond doesn't makes that much sense in practice. 
Given R:(X,Y,Z) and g(Z = '3')R != NULL, all this constraint does is tell that it is possible for a tuple {x,y, '3'} to exists in R. Not really useful. Especially when 
the condition include null values, such as g(Z = NULL)R != NULL which is basically our notation ways of telling that Z is nullable, something trivial to do in SQL.
Where this gets a bit more interesting might be when the guard express jointly null attributes. For instance, if we now had g(YZ = NULL)R != NULL, and so only allows Y and Z 
to be null together, then we get something that cannot be naturally precised in SQL. Which lead us to this first query enforcing joint nulls:
*/

IF EXISTS (SELECT * FROM R
			   WHERE (NEW.Z IS NOT NULL AND NEW.T IS NULL)
			   OR 	 (NEW.Z IS NULL AND NEW.T IS NOT NULL)
		) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;


/*
Simple enough. What this query does is check for tuples in which Z and T are NULL, but never together, which is what we want to avoid.
We naturally returns to the concept of constraint overlap in this section with the following example:
We have R:(X, Y, Z, T), with two conditions YZ jointly nullable and ZT also jointly nullable. All of the tuples expected from this table fit one of the following:
{{x, y, z, t},
{x, NULL, NULL, t},
{x, y, NULL, NULL},
{x, NULL, NULL, NULL}}
Not skipping the failing learning example, let's naively reproduce the last function:
*/

CREATE OR REPLACE FUNCTION transducer.CHECK_R_GJ_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
	IF EXISTS (SELECT * FROM transducer.R
			   WHERE (NEW.Y IS NOT NULL AND NEW.Z IS NULL)
			   OR 	 (NEW.Y IS NULL AND NEW.Z IS NOT NULL)
		) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.CHECK_R_GJ_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
	IF EXISTS (SELECT * FROM transducer.R
			   WHERE (NEW.Z IS NOT NULL AND NEW.T IS NULL)
			   OR 	 (NEW.Z IS NULL AND NEW.T IS NOT NULL)
		) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;
END;
$$;


/*
This doesn't work as any tuples of the form {x, NULL, NULL, t} or {x, y, NULL, NULL} violates the other constraint. And yet, we can't allow just leaving YZT as just nullable 
either as it would allows rows like {x, y, NULL, t} to get INSERTed, which is undesirable. A possible answer could be to ground all possible wrong cases and reject them.
A smarter answer would be to do the same for all valid ones instead. As intuitive and easy to implement this solution is, I still find it lacking in elegance. I would had 
more reservation if someone hadn't made a really neat algorithm which automatically ground all correct tuple disposition from a given set of guard constraint, so let's try
the following:
*/

CREATE OR REPLACE FUNCTION transducer.CHECK_R_GJ_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
	IF NOT EXISTS (SELECT NEW.*
			   WHERE (NEW.X IS NOT NULL AND NEW.Y IS NOT NULL AND NEW.Z IS NOT NULL AND NEW.T IS NOT NULL)
			   OR 	 (NEW.X IS NOT NULL AND NEW.Y IS NULL AND NEW.Z IS NULL AND NEW.T IS NOT NULL)
			   OR 	 (NEW.X IS NOT NULL AND NEW.Y IS NOT NULL AND NEW.Z IS NULL AND NEW.T IS NULL)
			   OR 	 (NEW.X IS NOT NULL AND NEW.Y IS NULL AND NEW.Z IS NULL AND NEW.T IS NULL)
		) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE JOINTLY NULL CONSTRAINT WITH %', NEW;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;
END;
$$;

/*
Inelegant as it is, I doubt that THAT many jointly null overlaps exist in practice anyway.
*/



/*Conditional Join Dependencies*/
/*
A relatively new introduction to the realm of database constraints, Conditional Join Dependencies (CJD) are constraints which only hold as long as each attribute covered are 
non-null. An example would be Person:(ssn, name, dep_name, dep_adress), with dep_name and dep_address nullable and dep_name -> dep_address. Some valid instances: 
{{'ssn1', 'John', 'dep1', 'depadd1'},
{'ssn2', 'Jane', 'dep2', 'depadd2'},
{'ssn3', 'Jovial', NULL, NULL}}
The FD still applies whenever the department attributes are non-null, thus rejecting INSERTs like {'ssn4', 'June', 'dep1', 'dep2'}.
To write as a function require thus to only check for the FD in the case where the concerned attribute are non-null. We can do it as such:
*/

IF EXISTS (SELECT * 
		   FROM R1, R2
		   WHERE (R2.Z IS NOT NULL AND R2.T IS NOT NULL 
				AND R1.Z = R2.Z 
				AND R1.T <> R2.T) 
				OR (R2.Z IS NULL AND R2.T IS NOT NULL) 
				OR (R2.Z IS NOT NULL AND R2.T IS NULL)) THEN
		RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT %', NEW;
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;

/*
Pretty straightforward; we catch any tuples violating the FD or any which are not jointly null. On that last point, given the existence of a FD, it would be highly 
problematic to allows tuples like {'x5', 'y5', NULL, 't1'} or {'x6', 'y6', 'z2', NULL}. FDs and null values have a long and complex history but from where we stand, this 
type of impious mixes are a no-go.
Similarly to what we did before for FDs, we want to see more complex scenarios involving CJDs.
Starting with the overlap used in my thesis' example, although a bit simplified:
Given R:(ssn, name, empid, hdate, dep_name), we have two conditional FDs
- fd[R](empid,hdate)(empid,hdate != NULL)
- fd[R](empid,dep_name)(empid,hdate,dep_name != NULL)
Here, the those constraints allows all of the following tuples:
{{'ssn1', 'John', 'id1', 'date1', 'dep1'},
{'ssn2', 'June', 'id2', 'date2', NULL},
{'ssn3', 'Joel', NULL, NULL, NULL}}
The underlying structure of this table is that of a person then specialized into an employee with empid, then specialized into a employee working for a specific department if 
a dep_name value exists.

*/



/*Inclusion Dependency*/
/*
This constraints concerns internal inclusion dependency of the sort that a manager attribute is a specialization of a person labeled with a ssn, and so,
all manager values must also exists in ssn. In other words, the set of values taken by manager is a subset, possibly equal, to that of ssn.*/





