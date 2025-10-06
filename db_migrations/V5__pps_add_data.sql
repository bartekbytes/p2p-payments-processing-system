/*
10 famous actors and actress
mixed gender and date of birth bucket
*/
INSERT INTO pps.users (first_name, last_name, date_of_birth, sex) 
VALUES

-- 50+ males
('Leonardo', 'DiCaprio', '1974-11-11', 'M'),
('Brad', 'Pitt', '1963-12-18', 'M'),

-- 50+ females
('Sandra', 'Bullock', '1964-07-26', 'F'),

-- 30–40 males
('Chris', 'Hemsworth', '1983-08-11', 'M'),
('Ryan', 'Gosling', '1980-11-12', 'M'),

-- 30–40 females
('Scarlett', 'Johansson', '1984-11-22', 'F'),
('Natalie', 'Portman', '1981-06-09', 'F'),

-- 20–30 males
('Timothée', 'Chalamet', '1995-12-27', 'M'),
('Tom', 'Holland', '1996-06-01', 'M'),

-- 20–30 female
('Zendaya', 'Coleman', '1996-09-01', 'F');



INSERT INTO pps.currencies (currency_code, currency_name) 
VALUES
('HKD', 'Hong Kong Dollar'),
('PHP', 'Philippine Peso'),
('USD', 'US Dollar'),
('EUR', 'Euro'),
('GBP', 'British Pound');



INSERT INTO pps.transaction_statuses (transaction_status)
VALUES
('completed'),
('pending'),
('failed');
