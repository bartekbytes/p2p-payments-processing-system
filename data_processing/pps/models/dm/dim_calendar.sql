{{ config(
    materialized='table'
) }}

SELECT
    date_id AS date_key,
    date_number,
    
    day_name,
    day_name_abbr,
    
    month_number,
    month_name,
    month_name_abbr,
    quarter_number,
    quarter_name,
    
    start_of_month,
    end_of_month,
    start_of_quarter,
    end_of_quarter,
    start_of_year,
    end_of_year,
    
    yyyymm,
    yyyymmdd,
    
    year_number,
    year_only,
    year_quarter,
    year_quarter_month,
    year_month,
    
    is_weekend
FROM {{ ref('calendar') }}