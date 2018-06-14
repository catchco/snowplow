{% set test_fields %}
    {% if target.name == 'bigquery' %}
        cast(user_custom_id as string) as user_custom_id,
        cast(inferred_user_id as string) as inferred_user_id,
        cast(user_snowplow_domain_id as string) as user_snowplow_domain_id,
        cast(user_snowplow_crossdomain_id as string) as user_snowplow_crossdomain_id,
        cast(app_id as string) as app_id,
        cast(engagement.bounced_page_views as int64) as bounced_page_views,
        cast(landing_page.url as string) as first_page_url,
        cast(marketing.medium as string) as marketing_medium,
        cast(marketing.source as string) as marketing_source,
        cast(marketing.term as string) as marketing_term,
        cast(marketing.campaign as string) as marketing_campaign,
        cast(marketing.content as string) as marketing_content,
        cast(referer.url as string) as referer_url,
        string(timestamp_trunc(session_start, second), '{{ var("snowplow:timezone") }}') as session_start,
        string(timestamp_trunc(session_end, second), '{{ var("snowplow:timezone") }}') as session_end,
        cast(session_id as string) as session_id,
        cast(engagement.time_engaged_in_s as int64) as time_engaged_in_s,
        cast(session_index as int64) as session_index
    {% else %}
        user_custom_id,
        inferred_user_id,
        user_snowplow_domain_id,
        user_snowplow_crossdomain_id,
        app_id,
        bounced_page_views,
        first_page_url,
        marketing_medium::text,
        marketing_source::text,
        marketing_term::text,
        marketing_campaign::text,
        marketing_content::text,
        referer_url::text,
        session_start,
        session_end,
        session_id,
        time_engaged_in_s,
        session_index
    {% endif %}
{% endset %}

{% set expected_fields %}
    {% if target.name == 'bigquery' %}
        cast(user_custom_id as string) as user_custom_id,
        cast(inferred_user_id as string) as inferred_user_id,
        cast(user_snowplow_domain_id as string) as user_snowplow_domain_id,
        cast(user_snowplow_crossdomain_id as string) as user_snowplow_crossdomain_id,
        cast(app_id as string) as app_id,
        cast(bounced_page_views as int64) as bounced_page_views,
        cast(first_page_url as string) as first_page_url,
        cast(marketing_medium as string) as marketing_medium,
        cast(marketing_source as string) as marketing_source,
        cast(marketing_term as string) as marketing_term,
        cast(marketing_campaign as string) as marketing_campaign,
        cast(marketing_content as string) as marketing_content,
        cast(referer_url as string) as referer_url,
        string(timestamp(session_start, '{{ var("snowplow:timezone") }}'), '{{ var("snowplow:timezone") }}') as session_start,
        string(timestamp(session_end, '{{ var("snowplow:timezone") }}'), '{{ var("snowplow:timezone") }}') as session_end,
        cast(session_id as string) as session_id,
        cast(time_engaged_in_s as int64) as time_engaged_in_s,
        cast(session_index as int64) as session_index
    {% else %}
        user_custom_id,
        inferred_user_id,
        user_snowplow_domain_id,
        user_snowplow_crossdomain_id,
        app_id,
        bounced_page_views,
        first_page_url,
        marketing_medium::text,
        marketing_source::text,
        marketing_term::text,
        marketing_campaign::text,
        marketing_content::text,
        referer_url::text,
        session_start,
        session_end,
        session_id,
        time_engaged_in_s,
        session_index
    {% endif %}
{% endset %}


with expected as (

    select {{ expected_fields }} from {{ ref('snowplow_sessions_expected') }}

),

actual as (

    select {{ test_fields }} from {{ ref('snowplow_sessions') }}

),

a_minus_b as (

  select * from expected
  except distinct
  select * from actual

),

b_minus_a as (

  select * from actual
  except distinct
  select * from expected

)

select * from a_minus_b
union all
select * from b_minus_a
