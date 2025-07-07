
--  mart_credit_metrics.sql
--  Calculates monthly default rates by grade, joined with macro indicators.


with base as (
  select
    date_trunc('month', issue_date)      as month,
    grade,
    count(*)                             as total_loans,
    sum(default_flag)                    as total_defaults
  from {{ ref('stg_loan_stats') }}
  group by 1, 2
),

rates as (
  select
    month,
    grade,
    total_loans,
    total_defaults,
    total_defaults * 1.0 / nullif(total_loans,0) as default_rate
  from base
)

select
  r.month,
  r.grade,
  r.total_loans,
  r.total_defaults,
  r.default_rate,
  m.unemployment_rate,
  m.gdp_level,
  m.cpi_level
from rates r
left join {{ ref('stg_macro') }} m
  on r.month = m.obs_date
order by r.month, r.grade
