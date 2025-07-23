{{ select_preamble }} {{ attributes }}
FROM transducer._POSITION{{ primary_suffix }}
NATURAL LEFT OUTER JOIN transducer._EMPDEP{{ secondary_suffix }}
{{ where | default('WHERE ssn IS NOT NULL AND dep_address IS NOT NULL') }}