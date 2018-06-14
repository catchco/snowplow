
{% set test_fields %}
    {% if target.name == 'bigquery' %}
        user_custom_id,
        user_snowplow_domain_id,
        user_snowplow_crossdomain_id,
        session_id,
        session_index,
        page_view_id,
        string(timestamp_trunc(page_view_start, second), '{{ var("snowplow:timezone") }}') as page_view_start,
        string(timestamp_trunc(page_view_end, second), '{{ var("snowplow:timezone") }}') as page_view_end,
        engagement.time_engaged_in_s,
        engagement.x_scroll_pct as horizontal_percentage_scrolled,
        engagement.y_scroll_pct as vertical_percentage_scrolled,
        page.url as page_url,
        marketing.medium as marketing_medium,
        marketing.source as marketing_source,
        marketing.term as marketing_term,
        marketing.content as marketing_content,
        marketing.campaign as marketing_campaign
    {% else %}
        user_custom_id,
        user_snowplow_domain_id,
        user_snowplow_crossdomain_id,
        session_id,
        session_index,
        page_view_id,
        page_view_end,
        page_view_start,
        time_engaged_in_s,
        horizontal_percentage_scrolled,
        vertical_percentage_scrolled,
        page_url,
        marketing_medium,
        marketing_source,
        marketing_term,
        marketing_content,
        marketing_campaign
    {% endif %}
{% endset %}

{% set expected_fields %}
    {% if target.name == 'bigquery' %}
        user_custom_id,
        user_snowplow_domain_id,
        user_snowplow_crossdomain_id,
        session_id,
        session_index,
        page_view_id,
        string(timestamp(page_view_start, '{{ var("snowplow:timezone") }}'), '{{ var("snowplow:timezone") }}') as page_view_start,
        string(timestamp(page_view_end, '{{ var("snowplow:timezone") }}'), '{{ var("snowplow:timezone") }}') as page_view_end,
        time_engaged_in_s,
        horizontal_percentage_scrolled,
        vertical_percentage_scrolled,
        page_url,
        marketing_medium,
        marketing_source,
        marketing_term,
        marketing_content,
        marketing_campaign
    {% else %}
        user_custom_id,
        user_snowplow_domain_id,
        user_snowplow_crossdomain_id,
        session_id,
        session_index,
        page_view_id,
        page_view_start,
        page_view_end,
        time_engaged_in_s,
        horizontal_percentage_scrolled,
        vertical_percentage_scrolled,
        page_url,
        marketing_medium,
        marketing_source,
        marketing_term,
        marketing_content,
        marketing_campaign

    {% endif %}
{% endset %}


with expected as (

    select {{ expected_fields }} from {{ ref('snowplow_page_views_expected') }}

),

actual as (

    select {{ test_fields }} from {{ ref('snowplow_page_views') }}

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


