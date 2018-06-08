
{% macro bigquery__snowplow_web_events_time() %}

{{ config(materialized='ephemeral') }}
select 1 as no_op

{% endmacro %}
