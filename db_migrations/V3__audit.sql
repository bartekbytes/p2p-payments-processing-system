CREATE TABLE pps_audit.loading_processes
(
    load_id SERIAL PRIMARY KEY,
    source_system_id INT, -- Id of a source system that loaded the given row, make in NULL-able for edge cases (manual user intervention, etc...)
    file_name VARCHAR(255), -- system details, like:file name for csv loading, Kafka topic details for streaming loading), field name not the most lucky one :(
    mode VARCHAR(50), -- loading mode
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    process_status VARCHAR(50) NOT NULL, -- final status of the process
    total_rows INT, -- total rows processed
    approved_rows INT, -- total rows inserted to DB
    rejected_rows INT, -- total rejected rows (roors)
    suspicious_cases INT, -- total suspicious transactions detected
    duration_seconds NUMERIC(10,2) -- process duration in seconds
) TABLESPACE pg_default;


CREATE TABLE pps_audit.transaction_errors
(
    id SERIAL PRIMARY KEY,
    load_id INT REFERENCES pps_audit.loading_processes(load_id), -- load_id from pps_audit.loading_processes
    line_number INT, -- in case of file loading, line number in the file where error occured
    -- below, start replication of fields from original Transaction table (no constraints here)
    transaction_id VARCHAR(50), 
    sender_id VARCHAR(50),
    receiver_id VARCHAR(50),
    amount NUMERIC(15,2),
    currency VARCHAR(3),
    timestamp TIMESTAMPTZ,
    status VARCHAR(20),
    -- end replication of fields from original Transaction table 
    error TEXT NOT NULL, -- detailed error message
    error_type VARCHAR(50), -- error type: schem, integrity, ...
    created_at TIMESTAMPTZ DEFAULT NOW() -- when record was created
) TABLESPACE pg_default;


CREATE TABLE pps_audit.suspicious_transactions
(
    id SERIAL PRIMARY KEY,
    load_id SERIAL NOT NULL REFERENCES pps_audit.loading_processes(load_id), -- load_id from pps_audit.loading_processes
    transaction_id VARCHAR(50) NOT NULL, -- transaction_id of the Transaction (BK)
    violation_reason TEXT NOT NULL, -- detailed information why violation of the rule happened
    violation_severity VARCHAR(20) NOT NULL DEFAULT 'medium', -- severity,defined in master data
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- when record was created
) TABLESPACE pg_default;