
with page_views as (

    select * from `dbt_dbanin.snowplow_page_views`
    where page_view_start > '2018-06-01'

),


sessions as (
  select
    domain_sessionid,
    min(page_view_start) as session_start,
    max(page_view_end) as session_end,
    sum(pings.time_engaged_in_s) as time_engaged_in_s,

    array_agg(struct(
        app_id,
        br_type,
        domain_userid,
        domain_sessionidx,
        user_id,
        user_ipaddress,
        dvce_ismobile,
        dvce_type,
        geo,
        utm,
        referrer
      )
      order by collector_tstamp asc
    )[safe_offset(0)] as details,

    array_agg(struct(
        page_view_id,
        page,
        referrer,
        pings,
        page_pings
      )
      order by collector_tstamp asc
    ) as pageviews

  from page_views
  group by 1

),

sessions_xf as (

  select
    domain_sessionid,

    details.domain_userid,
    details.domain_sessionidx,
    details.user_id,
    details.user_ipaddress,
    details.app_id,
    details.br_type,
    details.dvce_ismobile,
    details.dvce_type,
    details.geo,
    details.utm,

    session_start,
    session_end,

    struct(
      pageviews[safe_offset(0)].page.url_path as first_page_path,
      pageviews[safe_offset(array_length(pageviews) - 1)].page.url_path as exit_page_path,
      pageviews[safe_offset(0)].page.url as first_page_url,
      pageviews[safe_offset(array_length(pageviews) - 1)].page.url as exit_page_url
    ) as overview,

    array_length(pageviews) as count_pageviews,
    time_engaged_in_s,
    pageviews

  from sessions

)

select *
from sessions_xf
