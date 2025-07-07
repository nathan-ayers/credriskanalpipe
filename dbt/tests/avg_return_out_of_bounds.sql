-- this will fail if any row has avg_return < 0 or > 1
select *
from {{ ref('mart_market_metrics') }}
where avg_return < -0.2 OR avg_return > 0.2  