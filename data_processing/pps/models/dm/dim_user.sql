{{ config(
    materialized='table'
) }}

WITH baseline AS
(
SELECT
    user_id as user_key,
    first_name,
    last_name,
    date_of_birth,
    sex,
    EXTRACT(YEAR FROM AGE(now(), date_of_birth)) age_in_years
FROM {{ ref('users') }}
),

age_bucketing AS
(
    SELECT
        user_key,
        first_name,
        last_name,
        date_of_birth,
        sex,
        age_in_years,
        {{ dm_user_age_bucket('age_in_years', age_bucket_type='decade') }} age_bucket_decade,
        {{ dm_user_age_bucket('age_in_years', age_bucket_type='life_stage') }} age_bucket_life_stage,
        {{ dm_user_age_bucket('age_in_years', age_bucket_type='life_stage_name') }} age_bucket_life_stage_name
    FROM baseline
)

SELECT
    user_key,
    first_name,
    last_name,
    date_of_birth,
    sex,
    age_in_years,
    age_bucket_decade,
    age_bucket_life_stage,
    age_bucket_life_stage_name
FROM age_bucketing