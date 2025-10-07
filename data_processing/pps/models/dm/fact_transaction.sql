{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT
    COALESCE(cal.date_key, {{ var('unknown_member_date') }}) date_key,
    COALESCE(sdr.user_key, {{ var('unknown_member_id')}}) sender_key,
    COALESCE(rcv.user_key, {{ var('unknown_member_id')}}) receiver_key,
    COALESCE(cur.currency_key, {{ var('unknown_member_id') }}) currency_key,
    COALESCE(sts.transaction_status_key, {{ var('unknown_member_id') }}) transaction_status_key,
    amount,
    transaction_datetime, -- keeping for detailed analysis
    transaction_id -- keeping for tracking Transaction in Data sMart
FROM {{ ref('transactions') }} t
LEFT JOIN {{ ref('dim_calendar') }} cal
    ON t.transaction_date = cal.date_key
LEFT JOIN {{ ref('dim_user') }} sdr
    ON t.sender_id = sdr.user_key
LEFT JOIN {{ ref('dim_user') }} rcv
    ON t.receiver_id = rcv.user_key
LEFT JOIN {{ ref('dim_currency') }} cur
    ON t.currency = cur.currency_code
LEFT JOIN {{ ref('dim_transaction_status') }} sts
    ON t.status = sts.transaction_status

{% if is_incremental() %}
WHERE t.transaction_datetime > (SELECT COALESCE(MAX(transaction_datetime), {{ var('infinity_minus') }}) FROM {{ this }})
{% endif %}
