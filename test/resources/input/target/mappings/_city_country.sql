SELECT DISTINCT {{ attributes }}
FROM transducer._CITY_COUNTRY{{ primary_suffix }}
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY{{ secondary_suffix }}
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT{{ secondary_suffix }}
   NATURAL LEFT OUTER JOIN transducer._PERSON{{ secondary_suffix }}
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE{{ secondary_suffix }}
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL{{ secondary_suffix }}
{{ where | default('') }}
