-- Users table
CREATE TABLE pps.users 
(
    user_id SERIAL PRIMARY KEY, -- Surrogate Key
    first_name VARCHAR(50) NOT NULL, -- User's first name, required field
    last_name VARCHAR(50) NOT NULL, -- User's last name, required field
    date_of_birth DATE NOT NULL, -- User's date of birth, required field
    sex CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')) -- Gender, must be 'M' (male) or 'F' (female)
) TABLESPACE pg_default;



-- Currencies table
CREATE TABLE pps.currencies 
(
    currency_id SERIAL PRIMARY KEY, -- Surrogate Key
    currency_code CHAR(3) NOT NULL UNIQUE, -- ISO 4217 code (Business Key)
    currency_name VARCHAR(50) NOT NULL -- Full Currency name
) TABLESPACE pg_default;


-- Transaction Statuses table
CREATE TABLE pps.transaction_statuses
(
    transaction_status_id SERIAL PRIMARY KEY,
    transaction_status VARCHAR(50) UNIQUE NOT NULL
) TABLESPACE pg_default;


-- Transactions table
CREATE TABLE pps.transactions
(
    tx_id SERIAL PRIMARY KEY, -- Surrogate Key
    transaction_id VARCHAR(50) NOT NULL UNIQUE, -- Business Key
    sender_id INT NOT NULL REFERENCES pps.users(user_id), -- Id of the Transaction Sender
    receiver_id INT NOT NULL REFERENCES pps.users(user_id), -- Id of the Transaction Receiver
    amount NUMERIC(18,2) NOT NULL CHECK (amount > 0), -- Amount sent
    currency CHAR(3) NOT NULL, -- In which Currency Amountwas sent (ISO 4217 code)
    timestamp TIMESTAMPTZ NOT NULL, -- When Transaction was registered
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'completed', 'failed')), -- Status of the Transaction
    CHECK (sender_id <> receiver_id), -- Sender and Receiver can't be the same person
    CHECK (transaction_id LIKE 'tx%'), -- Must start with "tx"
    -- audit fields
    load_id INT -- Id of a loading process that loaded the given row into the table, make it NULL-able for edge cases (manual insert, etc...)
) TABLESPACE pg_default;
--PARTITION BY RANGE (timestamp); -- Partitioned by timestamp (monthly partitions)

/*
As this table will grow rapidly over time,
a good approach is to consider to partition this table.

As Transaction tables are, generally speaking, a good example of time-series tables,
partitioning should be done over a date field. Here we have a timestamp field, not ideal, not that bad.
This could initially be used but maybe better solution can be found later.

*/
-- Monthly Partitions for 2025
/*
CREATE TABLE pps.transactions_default PARTITION OF pps.transactions DEFAULT TABLESPACE pg_default;
CREATE TABLE pps.transactions_202501 PARTITION OF pps.transactions FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2025-02-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202502 PARTITION OF pps.transactions FOR VALUES FROM ('2025-02-01 00:00:00+00') TO ('2025-03-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202503 PARTITION OF pps.transactions FOR VALUES FROM ('2025-03-01 00:00:00+00') TO ('2025-04-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202504 PARTITION OF pps.transactions FOR VALUES FROM ('2025-04-01 00:00:00+00') TO ('2025-05-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202505 PARTITION OF pps.transactions FOR VALUES FROM ('2025-05-01 00:00:00+00') TO ('2025-06-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202506 PARTITION OF pps.transactions FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202507 PARTITION OF pps.transactions FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202508 PARTITION OF pps.transactions FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202509 PARTITION OF pps.transactions FOR VALUES FROM ('2025-09-01 00:00:00+00') TO ('2025-10-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202510 PARTITION OF pps.transactions FOR VALUES FROM ('2025-10-01 00:00:00+00') TO ('2025-11-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202511 PARTITION OF pps.transactions FOR VALUES FROM ('2025-11-01 00:00:00+00') TO ('2025-12-01 00:00:00+00') TABLESPACE pg_default;
CREATE TABLE pps.transactions_202512 PARTITION OF pps.transactions FOR VALUES FROM ('2025-12-01 00:00:00+00') TO ('2026-01-01 00:00:00+00') TABLESPACE pg_default;
*/

-- Indexes also need to be proposed, depends on the usage