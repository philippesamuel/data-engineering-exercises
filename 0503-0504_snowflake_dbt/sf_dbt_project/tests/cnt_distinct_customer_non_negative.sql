-- Fail if distinct order count is negative
select *
from {{ ref('fact_orders_geography') }}
where CNT_DISTINCT_CUSTOMER < 0

