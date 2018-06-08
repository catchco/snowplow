
{% macro bigquery__snowplow_web_events() %}

{{
    config(
        materialized='ephemeral'
    )
}}

select 1 as no_op

{% endmacro %}
