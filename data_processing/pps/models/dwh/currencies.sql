{{ config(
    materialized='table'
) }}


SELECT
    currency_id::INT,
    currency_code::CHAR(3),
    currency_name::VARCHAR(50)
FROM {{ source('pps', 'currencies')}}