-- TODO: Maybe all select distinct
{{ select_preamble }} {{ attributes }}
FROM transducer._EMPDEP{{ primary_suffix }}
NATURAL LEFT OUTER JOIN transducer._POSITION{{ secondary_suffix }}
{{ where | default('WHERE ssn IS NOT NULL AND dep_address IS NOT NULL') }}
-- TODO: Programatically generate the where is not null with primary keys from all join tables