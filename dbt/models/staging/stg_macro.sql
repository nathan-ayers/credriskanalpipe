
--  stg_macro.sql
--  Clean and type your macroeconomic series: unemployment, GDP, CPI.


select
  cast("__index_level_0__" as date)    as obs_date,
  cast(unemployment as numeric)           as unemployment_rate,
  cast(gdp as numeric)                    as gdp_level,
  cast(cpi as numeric)                    as cpi_level
from read_parquet('../data/staging/macro.parquet')