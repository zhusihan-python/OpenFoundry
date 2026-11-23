\getenv reader_password READER_PASSWORD
\getenv other_password OTHER_PASSWORD
CREATE ROLE orders_reader LOGIN PASSWORD :'reader_password';
CREATE ROLE other_reader LOGIN PASSWORD :'other_password';
CREATE SCHEMA sales;
CREATE TABLE sales.orders (order_id text PRIMARY KEY, amount_cents bigint NOT NULL, batch text NOT NULL);
GRANT USAGE ON SCHEMA sales TO orders_reader, other_reader;
GRANT SELECT ON sales.orders TO orders_reader;
