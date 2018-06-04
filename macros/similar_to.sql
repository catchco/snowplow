{% macro similar_to(values) %}
  {{ adapter_macro('snowplow.similar_to', values) }}
{% endmacro %}

{% macro default__similar_to(values) %}
    similar to '%({{ values | join("|") }})%'
{%- endmacro %}

{% macro snowflake__similar_to(values) %}
    rlike '.*({ {values | join("|") }}).*'
{% endmacro %}
