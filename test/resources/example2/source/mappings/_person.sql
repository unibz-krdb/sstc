SELECT {{ attributes }}
FROM transducer._PERSON{{ primary_suffix }}
{{ where | default('') }}