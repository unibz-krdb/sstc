SELECT DISTINCT {{ attributes }}
FROM transducer._POSITION{{ primary_suffix }}
NATURAL LEFT OUTER JOIN transducer._EMPDEP{{ secondary_suffix }}
{{ where | default('') }}