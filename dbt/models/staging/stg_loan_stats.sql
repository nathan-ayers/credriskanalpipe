--  stg_loan_stats.sql
--  A “staging” table: lightly cleaned and standardized

WITH raw AS (

SELECT * 
FROM read_parquet('../data/staging/loan_stats.parquet')
)

SELECT
    cast(id AS integer) AS loan_id,
    cast(loan_amnt as numeric) AS loan_amount,
    term AS term_months,
    trim(grade) AS grade,
    cast(
        replace(trim(int_rate), '%', '') 
        as numeric
        ) / 100 as interest_rate,
    cast(annual_inc as numeric) AS annual_income,
    cast(dti as numeric) AS debt_to_income_ratio,
    strptime(concat('01-', trim(issue_d)), '%d-%b-%Y')::date as issue_date,
    case
        when lower(loan_status) = 'charged off' then 1
        when lower(loan_status) = 'default'     then 1
    else 0
end as default_flag
FROM raw
WHERE loan_amount IS NOT NULL
