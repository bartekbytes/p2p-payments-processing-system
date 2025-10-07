{{ config(
    materialized='table'
) }}


SELECT
    user_id::INT,
    first_name::VARCHAR(50),
    last_name::VARCHAR(50),
    date_of_birth::DATE,
    sex::CHAR(1)
FROM {{ source('pps', 'users')}}