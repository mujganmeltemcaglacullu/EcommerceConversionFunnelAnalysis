 ---Görev2
 select timestamp_micros(event_timestamp) as event_timestamp, user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id, event_name, geo.country as country, device.category, traffic_source.source as source, traffic_source.medium as medium, traffic_source. name as campaign
 FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where 
  event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info','add_payment_info', 'purchase') AND _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
 limit 1000;

---Görev 3
with base as (
  select timestamp_micros(event_timestamp) as event_timestamp, 
  date(timestamp_micros(event_timestamp)) as event_date, 
  user_pseudo_id, --anonim kullanıcı id
  (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id, 
  event_name,  
  traffic_source.source as source, 
  traffic_source.medium as medium, 
  traffic_source. name as campaign
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
   where event_name IN ('session_start',--Sitede oturum başlangıcı
  'add_to_cart', --Sepete ürün ekleme
  'begin_checkout',--Ödeme başlangıcı
  'purchase') --Satın alma  
  AND _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
),
session_events as(
select base.event_date, base.source, base.campaign, base.medium, concat (user_pseudo_id, '-' ,session_id) user_session_id,
max(case when event_name='add_to_cart' then 1 else 0 end) as has_cart,
max(case when event_name='begin_checkout' then 1 else 0 end) as has_checkout,
max(case when event_name='purchase' then 1 else 0 end) as has_purchase,
max(case when event_name='session_start' then 1 else 0 end) as has_session
from base
group by 1,2,3,4,5

)
select 
  event_date,
  source, 
  medium, 
  campaign,
  count(distinct user_session_id) as user_sessions_count,
  SAFE_DIVIDE(sum(has_cart),count(distinct user_session_id)) as visit_to_cart,
  SAFE_DIVIDE(sum(has_checkout),count(distinct user_session_id)) as visit_to_checkout,
  SAFE_DIVIDE(sum(has_purchase),count(distinct user_session_id)) as visit_to_purchase,
from session_events
where has_session =1
group by 1,2,3,4
order by 1,2,3;

---Görev 4 / Acilis sayfalari arasındaki donusum karsilastirmasi

with base as(
select 
  user_pseudo_id,
  (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id,
  event_name,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
  timestamp_micros(event_timestamp) as event_timestamp
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where _TABLE_SUFFIX BETWEEN '20200101' AND '20201231' and event_name in ('purchase','session_start')
),
cleaned_path as(
select 
  user_pseudo_id,
  session_id,
  event_name,
  event_timestamp,
  regexp_extract(page_location, r'(?:\w+\:\/\/)?[^\/]+\/([^\?#]*)') AS page_path
from base
where event_name = 'session_start'
),session_flag as(
select 
  cp.user_pseudo_id, 
  cp.session_id,
  cp.page_path,
  max(case when b.event_name = 'purchase'then 1 else 0 end) as has_purchase
from cleaned_path cp
join base b
on cp.user_pseudo_id = b.user_pseudo_id
and cp.session_id = b.session_id
group by cp.user_pseudo_id, cp.session_id, cp.page_path
)
select page_path, count(distinct user_pseudo_id || cast(session_id as string)) as user_session_id,
sum(has_purchase) as count_purchase, safe_divide(sum(has_purchase) , count(distinct user_pseudo_id || cast(session_id as string))) as purchase_rate
from session_flag
--where page_path = 'Google+Redesign/Apparel'
group by page_path
order by purchase_rate desc