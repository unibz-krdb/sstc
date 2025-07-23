SELECT DISTINCT {{ attributes }}
FROM transducer._DEPARTMENT_CITY{{ primary_suffix }}
   NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
   NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
   NATURAL LEFT OUTER JOIN transducer._PERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
{{ where | default('') }}
