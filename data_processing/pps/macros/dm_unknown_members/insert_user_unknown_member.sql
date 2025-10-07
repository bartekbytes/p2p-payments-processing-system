{% macro insert_user_unknown_member() %}

    INSERT INTO {{ this }} 
    (
        user_key,
        first_name,
        last_name,
        date_of_birth,
        sex
    )
    VALUES
    (
        {{ var('unknown_member_id') }},
        {{ var('unknown_member_string') }},
        {{ var('unknown_member_string') }},
        {{ var('unknown_member_date') }},
        'X'
    )

{% endmacro %}