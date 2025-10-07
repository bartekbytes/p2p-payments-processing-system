{% macro insert_transaction_status_unknown_member() %}

    INSERT INTO {{ this }} 
    (
        transaction_status_key,
        transaction_status
    )
    VALUES
    (
        {{ var('unknown_member_id') }},
        {{ var('unknown_member_string') }}
    )

{% endmacro %}