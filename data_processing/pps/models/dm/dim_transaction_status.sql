{{ config(
    materialized='table'
) }}

SELECT
    transaction_status_id AS transaction_status_key,
    transaction_status
FROM {{ ref('transaction_statuses') }}