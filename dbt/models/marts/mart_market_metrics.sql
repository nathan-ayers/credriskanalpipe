-- mart_market_metrics.sql
-- calculate daily returns per ticker

with daily as (
    select
        ticker,
        price_date,
        (close_price - lag(close_price) over (partition by ticker order by price_date))
         / lag(close_price) over (partition by ticker order by price_date)
        as daily_return
    from {{ ref('stg_bank_prices') }}
),

monthly as (
    select
        ticker,
        date_trunc('month', price_date) as month,
        avg(daily_return) as avg_return,
        stddev(daily_return) as volatility
    from daily
    group by 1, 2
    ORDER BY month
)

select * from monthly

