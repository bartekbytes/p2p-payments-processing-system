{{ config(
    materialized='table'
) }}

SELECT
    currency_id AS currency_key,
    currency_code,
    currency_name
FROM {{ ref('currencies') }}