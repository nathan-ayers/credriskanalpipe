{{ config(materialized="view") }}

-- Bank prices from each ticker Parquet, unioned together
with bac as (
  select
    'BAC'         as ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
  from read_parquet('../data/staging/BAC.parquet')
),

c as (
  select
    'C'           as ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
  from read_parquet('../data/staging/C.parquet')
),

jpm as (
  select
    'JPM'         as ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
  from read_parquet('../data/staging/JPM.parquet')
),

wfc as (
  select
    'WFC'         as ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
  from read_parquet('../data/staging/WFC.parquet')
)

select * from bac
union all
select * from c
union all
select * from jpm
union all
select * from wfc
