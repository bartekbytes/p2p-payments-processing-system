{% macro insert_currency_unknown_member() %}

    INSERT INTO {{ this }} 
    (
        currency_key,
        currency_code,
        currency_name
    )
    VALUES
    (
        {{ var('unknown_member_id') }},
        {{ var('unknown_member_string') }},
        {{ var('unknown_member_string') }}
    )

{% endmacro %}