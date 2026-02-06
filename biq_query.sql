--------------------------OVERVIEW PAGE-------------------------------
with sessions as
(
SELECT
  PARSE_DATE('%Y%m%d', event_date)as event_date,
  --event_name,
  t.device.category as device_type,
  geo.country as country,
  CASE 
      when REGEXP_REPLACE(t.traffic_source.source, r'[<>()]', '') = 'data deleted' then 'Other'
      else REGEXP_REPLACE(t.traffic_source.source, r'[<>()]', '') end as source,
  CASE 
       when REGEXP_REPLACE(t.traffic_source.medium, r'[<>()]', '') IN ('data deleted','none') then 'Other' 
       else REGEXP_REPLACE(t.traffic_source.medium, r'[<>()]', '') end as medium,
    (select ep.value.string_value
     from unnest(event_params) as ep
     where  ep.key = 'page_location')  as landing_page,
  count(DISTINCT user_pseudo_id) total_users,
  count(distinct case when event_name = 'first_visit' then user_pseudo_id end) as new_users,
  sum(case when event_name = 'session_start' then 1 else 0 end) as sessions,
  count(case when event_name = 'page_view' then user_pseudo_id end) as page_views,
  count(case when event_name = 'session_start' and 
        (select ep.value.int_value from unnest(event_params) ep 
                where  ep.key = 'engaged_session_event') =1 then 1 end) as engaged_sessions,
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t
group by 1,2,3,4,5,6--,7
),
ecom as
(SELECT
  PARSE_DATE('%Y%m%d', event_date)as event_date,
  --event_name,
  t.device.category as device_type,
  geo.country as country,
  CASE 
      when REGEXP_REPLACE(t.traffic_source.source, r'[<>()]', '') = 'data deleted' then 'Other'
      else REGEXP_REPLACE(t.traffic_source.source, r'[<>()]', '') end as source,
  CASE 
       when REGEXP_REPLACE(t.traffic_source.medium, r'[<>()]', '') IN ('data deleted','none') then 'Other' 
       else REGEXP_REPLACE(t.traffic_source.medium, r'[<>()]', '') end as medium,
  items.item_name,
  (select ep.value.string_value
     from unnest(event_params) as ep
     where  ep.key = 'page_location')  as landing_page,
  count(case when event_name = 'view_item' then user_pseudo_id end) as view_item,
  count(case when event_name = 'add_to_cart' then user_pseudo_id end) ad_to_cart,
  count(case when event_name = 'begin_checkout' then user_pseudo_id end) check_out,
  count(case when event_name = 'purchase' then 1 end) as purchase_events,
  sum(case when event_name = 'purchase' then t.ecommerce.purchase_revenue end) as revenue,
  count(distinct case when event_name = 'purchase' then t.ecommerce.transaction_id end) as transactions_or_purchases
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t,
unnest(items) as items
group by 1,2,3,4,5,6,7--,8
)

select event_date,
       --event_name,
      device_type,
      country,
      source,
      medium,
      landing_page,
      item_name as item_name,
      sum(case when new_rank = 1 then sessions else 0 end ) as sessions, 
      sum(case when new_rank = 1 then total_users else 0 end) as total_users, 
      sum(case when new_rank = 1 then new_users else 0 end) as new_users, 
      sum(case when new_rank = 1 then page_views else 0 end)as page_view, 
      sum(case when new_rank = 1 then  engaged_sessions else 0 end) as engaged_session,
      sum(view_item) as view_item,
      sum(ad_to_cart) as ad_to_cart,
      sum(check_out) as check_out,
      sum(purchase_events) as purchase_events,
      sum(revenue) as revenue,
      sum(transactions_or_purchases) as transactions_or_purchases
from(
select 
      s.event_date,
      --s.event_name,
      s.device_type,
      s.country,
      s.source,
      s.medium,
      s.landing_page,
      e.item_name as item_name,
      sum(s.total_users) as total_users,
      sum(s.new_users) as new_users,
      sum(s.sessions) as sessions,
      DENSE_RANK() over (partition by s.event_date,s.device_type,s.country,s.source,s.medium,s.landing_page order by e.item_name desc) as new_rank,
      sum(s.page_views) as page_views,
      sum(s.engaged_sessions) as engaged_sessions,
      sum(e.view_item) as view_item,
      sum(e.ad_to_cart) as ad_to_cart,
      sum(e.check_out) as check_out,
      sum(e.purchase_events) as purchase_events,
      sum(e.revenue) as revenue,
      sum(e.transactions_or_purchases) as transactions_or_purchases
from sessions s
left join ecom e on 
s.event_date = e.event_date
and s.source = e.source
and s.device_type = e.device_type
and s.country = e.country
and s.medium = e.medium
and s.landing_page = e.landing_page
group by 1,2,3,4,5,6,7--,8
)
group by 1,2,3,4,5,6,7--,8
limit 10;

