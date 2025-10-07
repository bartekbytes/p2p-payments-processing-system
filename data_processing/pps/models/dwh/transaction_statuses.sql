{{ config(
    materialized='table'
) }}


SELECT
    transaction_status_id::INT,
    transaction_status::VARCHAR(50)
FROM {{ source('pps', 'transaction_statuses')}}