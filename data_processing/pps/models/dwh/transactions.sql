{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}


WITH source_transactions AS 
(
    SELECT
        tx_id::INT,
        transaction_id::VARCHAR(50),
        sender_id::INT,
        receiver_id::INT,
        amount::NUMERIC(18,2),
        currency::CHAR(3),
        timestamp::TIMESTAMPTZ AS transaction_datetime, 
        status::VARCHAR(20),
        load_id::INT
    FROM {{ source('pps', 'transactions') }}
),

transaction_date AS
(
    SELECT
        tx_id,
        transaction_id,
        sender_id,
        receiver_id,
        amount,
        currency,
        transaction_datetime,
        CAST(transaction_datetime AS DATE) AS transaction_date,
        status,
        load_id
    FROM source_transactions
)

SELECT
    tx_id,
    transaction_id,
    sender_id,
    receiver_id,
    amount,
    currency,
    transaction_datetime,
    transaction_date,
    status,
    load_id
FROM transaction_date

{% if is_incremental() %}
WHERE transaction_datetime > (SELECT COALESCE(MAX(transaction_datetime), {{ var('infinity_minus')}} ) FROM {{ this }})
{% endif %}
