select *
from {{ ref('mart_market_metrics') }}
where volatility < 0