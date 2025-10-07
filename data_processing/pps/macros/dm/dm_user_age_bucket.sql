{% macro dm_user_age_bucket(age_in_years, age_bucket_type="decade") %}

    CASE
    
        {% if age_bucket_type == "decade" %}

            WHEN {{ age_in_years }} BETWEEN 0 AND 9   THEN '0-9'
            WHEN {{ age_in_years }} BETWEEN 10 AND 19 THEN '10-19'
            WHEN {{ age_in_years }} BETWEEN 20 AND 29 THEN '20-29'
            WHEN {{ age_in_years }} BETWEEN 30 AND 39 THEN '30-39'
            WHEN {{ age_in_years }} BETWEEN 40 AND 49 THEN '40-49'
            WHEN {{ age_in_years }} BETWEEN 50 AND 59 THEN '50-59'
            WHEN {{ age_in_years }} BETWEEN 60 AND 69 THEN '60-69'
            WHEN {{ age_in_years }} >= 70             THEN '70+'
            ELSE {{ var('unknown_member_string') }}
        
        {% elif age_bucket_type == "life_stage" %}

            WHEN {{ age_in_years }} < 18              THEN '<18'
            WHEN {{ age_in_years }} BETWEEN 18 AND 24 THEN '18-24'
            WHEN {{ age_in_years }} BETWEEN 25 AND 34 THEN '25-34'
            WHEN {{ age_in_years }} BETWEEN 35 AND 44 THEN '35-44'
            WHEN {{ age_in_years }} BETWEEN 45 AND 54 THEN '45-54'
            WHEN {{ age_in_years }} BETWEEN 55 AND 64 THEN '55-64'
            WHEN {{ age_in_years }} >= 65             THEN '65+'
            ELSE {{ var('unknown_member_string') }}

        {% elif age_bucket_type == "life_stage_name" %}
        
            WHEN {{ age_in_years }} < 18              THEN 'Children'
            WHEN {{ age_in_years }} BETWEEN 18 AND 24 THEN 'Young Adults'
            WHEN {{ age_in_years }} BETWEEN 25 AND 34 THEN 'Early Career'
            WHEN {{ age_in_years }} BETWEEN 35 AND 44 THEN 'Mid Career / Young Families'
            WHEN {{ age_in_years }} BETWEEN 45 AND 54 THEN 'Established'
            WHEN {{ age_in_years }} BETWEEN 55 AND 64 THEN 'Pre-Retirement'
            WHEN {{ age_in_years }} >= 65             THEN 'Retirement / Seniors'
            ELSE {{ var('unknown_member_string') }}

        {% endif %}
    
    END

{% endmacro %}