
--  stg_bank_prices.sql
--  Consolidate all five bank price Parquet files into one clean staging table.


with jpm as (
  select
    'JPM'           as ticker,
    Date            as price_date,
    Open            as open_price,
    High            as high_price,
    Low             as low_price,
    Close           as close_price,
    Volume          as volume
  from read_parquet('../data/staging/JPM.parquet')
),

bac as (
  select
    'BAC'           as ticker,
    Date            as price_date,
    Open            as open_price,
    High            as high_price,
    Low             as low_price,
    Close           as close_price,
    Volume          as volume
  from read_parquet('../data/staging/BAC.parquet')
),

c as (
  select
    'C'             as ticker,
    Date            as price_date,
    Open            as open_price,
    High            as high_price,
    Low             as low_price,
    Close           as close_price,
    Volume          as volume
  from read_parquet('../data/staging/C.parquet')
),

wfc as (
  select
    'WFC'           as ticker,
    Date            as price_date,
    Open            as open_price,
    High            as high_price,
    Low             as low_price,
    Close           as close_price,
    Volume          as volume
  from read_parquet('../data/staging/WFC.parquet')
),

gs as (
  select
    'GS'            as ticker,
    Date            as price_date,
    Open            as open_price,
    High            as high_price,
    Low             as low_price,
    Close           as close_price,
    Volume          as volume
  from read_parquet('../data/staging/GS.parquet')
)

select * from jpm
union all
select * from bac
union all
select * from c
union all
select * from wfc
union all
select * from gs
