SELECT {{ attributes }}
FROM transducer._PERSON_EMAIL{{ primary_suffix }}
NATURAL LEFT OUTER JOIN transducer._P{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PE{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PE_HDATE{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PED{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PED_DEPT{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER{{ secondary_suffix }}
NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE{{ secondary_suffix }}
{{ where | default('') }};