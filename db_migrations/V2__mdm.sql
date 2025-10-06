-- Source Systems table
CREATE TABLE pps_mdm.source_systems
(
    source_system_id SERIAL PRIMARY KEY, -- Surrogate Key
    source_system_name VARCHAR(50) UNIQUE NOT NULL, -- Source System name
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) TABLESPACE pg_default;

-- Insert data into Source Systems table
INSERT INTO pps_mdm.source_systems
(
    source_system_name
)
VALUES
    ('transactions_batch_loading'),
    ('transactions_stream_loading');

----------------------------------------------------

-- Transaction Validation Rules table
CREATE TABLE pps_mdm.transaction_validation_rules 
(
    id SERIAL PRIMARY KEY, -- Surrogate Key
    rule_type VARCHAR(50) UNIQUE NOT NULL, -- Rule descriptive name, like: amount_threshold, sender_blacklist, ...
    rule_description VARCHAR(300) NULL, -- Rule description, with deeper explanation
    params JSONB NOT NULL, -- Definition of the Rule, stored as JSON, parsed on client side
    violation_severity VARCHAR(20) NOT NULL CHECK (violation_severity IN ('low', 'medium', 'high', 'critical')) , -- Severity of the Rule violation
    active BOOLEAN NOT NULL DEFAULT TRUE,  -- enable/disable rule
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- possible extension: a historical table of Rule changes
    -- (or just store in the same table with ValidFrom, ValidTo fields)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) TABLESPACE pg_default;

-- Optional index for faster lookup
CREATE INDEX idx_rules_active ON pps_mdm.transaction_validation_rules(active);


INSERT INTO pps_mdm.transaction_validation_rules (rule_type, rule_description, params, violation_severity, active)
VALUES 
(
    'amount_threshold',
    'Rule detects and flags if the Amount of the Transaction is above a declared Threshold',
    '{"threshold": 10000}',
    'high',
    TRUE
),
(
    'sender_blacklist',
    'Rule detects and flags if the Transaction Sender is on a declared Blacklist',
    '{"blacklist": []}',
    'critical',
    TRUE
);



-- Calendar table, create the table on the fly using SQL stmt
CREATE TABLE pps_mdm.calendar AS
SELECT cal.*
FROM
(
    SELECT 
	    datum AS date_id,
	    TO_CHAR(datum,'yyyymmdd')::INT AS date_number,

	    TRIM(TO_CHAR(datum,'Day')) AS day_name,
	    TRIM(TO_CHAR(datum,'Dy')) AS day_name_abbr,

    	EXTRACT(MONTH FROM datum) AS month_number,
    	TRIM(TO_CHAR(datum,'Month')) AS month_name,
    	TRIM(TO_CHAR(datum,'Mon')) AS month_name_abbr,
    	EXTRACT(quarter FROM datum) AS quarter_number,
    	TRIM(CONCAT('Q',EXTRACT(quarter FROM datum))) quarter_name,
	
	    DATE_TRUNC('month', datum)::date AS start_of_month,
	    (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS end_of_month,
	    DATE_TRUNC('quarter',datum)::DATE AS start_of_quarter,
	    (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS end_of_quarter,
	    DATE_TRUNC('YEAR',datum)::DATE AS start_of_year,
    	(DATE_TRUNC('YEAR',datum)::DATE +INTERVAL '1 YEAR - 1 day')::DATE AS end_of_year,
    
	    TRIM(TO_CHAR(datum,'yyyymm')) AS yyyymm,
	    TRIM(TO_CHAR(datum,'yyyymmdd')) AS yyyymmdd,

	    EXTRACT(isoyear FROM datum) AS year_number,
	    TRIM(TO_CHAR(datum, 'yyyy')) year_only,
	    TRIM(CONCAT(EXTRACT(year from datum),'-Q',EXTRACT(quarter FROM datum))) year_quarter,
	    TRIM(CONCAT(EXTRACT(year FROM datum),'-Q',EXTRACT(quarter FROM datum),'-',TO_CHAR(datum,'Mon'))) year_quarter_month,
	    TRIM(CONCAT(EXTRACT(year FROM datum),'-',TO_CHAR(datum,'Mon')))  year_month,
    
	    CASE 
		    WHEN EXTRACT(isodow FROM datum) IN (6,7) 
    			THEN 1 
			    ELSE 0 
	    END AS is_weekend 
    FROM
    (
	    SELECT datum::date 
	    FROM GENERATE_SERIES
	    (
    		DATE '2023-01-01',
		    DATE '2030-12-31',
		    INTERVAL '1 day'
	    ) AS datum
    ) dates_series
) cal;

