{% macro insert_calendar_unknown_member() %}

    INSERT INTO {{ this }} 
    (
		date_key,
		date_number,

		day_name,
		day_name_abbr,

		month_number,
		month_name,
		month_name_abbr,
		quarter_number,
		quarter_name,

	
		start_of_month,
		end_of_month,
		start_of_quarter,
		end_of_quarter,
		start_of_year,
		end_of_year,
	
		yyyymm,
		yyyymmdd,

		year_number,
		year_only,
		year_quarter,
		year_quarter_month,
		year_month,

		is_weekend 
    )
    VALUES
    (
        {{ var('unknown_member_date') }}, -- date_id
        {{ var('unknown_member_id') }}, -- date_number

        {{ var('unknown_member_string') }}, -- day_name
        {{ var('unknown_member_string') }}, -- day_name_abbr

	    {{ var('unknown_member_number') }}, -- month_number,
	    {{ var('unknown_member_string') }}, -- month_name,
	    {{ var('unknown_member_string') }}, -- month_name_abbr,
	    {{ var('unknown_member_number') }}, -- quarter_number,
	    {{ var('unknown_member_string') }}, -- quarter_name,

	    {{ var('unknown_member_date') }}, -- start_of_month,
	    {{ var('unknown_member_date') }}, -- end_of_month,
	    {{ var('unknown_member_date') }}, -- start_of_quarter,
	    {{ var('unknown_member_date') }}, -- end_of_quarter,
	    {{ var('unknown_member_date') }}, -- start_of_year,
	    {{ var('unknown_member_date') }}, -- end_of_year,

	    {{ var('unknown_member_number') }}, -- yyyymm,
	    {{ var('unknown_member_number') }}, -- yyyymmdd,

	    {{ var('unknown_member_number') }}, -- year_number,
	    {{ var('unknown_member_string') }}, -- year_only,
	    {{ var('unknown_member_string') }}, -- year_quarter,
	    {{ var('unknown_member_string') }}, -- year_quarter_month,
	    {{ var('unknown_member_string') }}, -- year_month,

        {{ var('unknown_member_number') }} -- is_weekend
    )

{% endmacro %}