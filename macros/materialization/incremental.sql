
{% macro build_merge_condition(unique_key, source, dest) -%}
    {%- if unique_key -%}
        SOURCE.{{ unique_key }} = DEST.{{ unique_key }}
    {%- else -%}
        FALSE
    {%- endif -%}
{%- endmacro -%}

{% macro build_merge_statement(target, sql, unique_key, dest_columns) -%}

    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(', ') -%}
    {%- set merge_condition = build_merge_condition(unique_key) -%}

    merge into {{ target }} as DEST
    using (
        {{ sql }}
    ) as SOURCE
    on {{ merge_condition }}

    {% if unique_key -%}
        when matched then
        update set
        {% for column in dest_columns -%}
            {{ column.name }} = SOURCE.{{ column.name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {%- endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}

{% materialization incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% if non_destructive_mode %}
    {{ log("--non-destructive is not supported on BigQuery, and will be ignored", info=True) }}
  {% endif %}

  {%- set identifier = model['name'] -%}

  {%- set existing_relations = adapter.list_relations(schema=schema) -%}
  {%- set old_relation = adapter.get_relation(relations_list=existing_relations,
                                              schema=schema, identifier=identifier) -%}

  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, type='table') -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

  {%- set should_drop = (full_refresh_mode or exists_not_as_table) -%}
  {%- set force_create = (full_refresh_mode) -%}

  -- setup
  {% if old_relation is none -%}
    -- noop
  {%- elif should_drop -%}
    {{ adapter.drop_relation(old_relation) }}
    {%- set old_relation = none -%}
  {%- endif %}

  -- build model
  {% if force_create or old_relation is none -%}
    {%- call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}
     {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
     {%- call statement('main') -%}
       {{ build_merge_statement(target_relation, sql, unique_key, dest_columns) }}
     {% endcall %}
  {%- endif %}

{%- endmaterialization %}
