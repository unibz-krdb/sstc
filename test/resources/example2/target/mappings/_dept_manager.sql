SELECT DISTINCT {{ attributes }}
FROM transducer._DEPT_MANAGER{{ primary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PED_DEPT{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PED{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._P{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PE{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PE_HDATE{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PERSON_EMAI{{ secondary_suffix }}
{{ where | default('') }}