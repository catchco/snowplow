
{% macro bigquery__snowplow_web_events_scroll_depth() %}

{{ config(materialized='ephemeral') }}
select 1 as no_op

{% endmacro %}
