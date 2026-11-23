\getenv reader_password READER_PASSWORD
\getenv other_password OTHER_PASSWORD
CREATE ROLE orders_reader LOGIN PASSWORD :'reader_password';
CREATE ROLE other_reader LOGIN PASSWORD :'other_password';
CREATE SCHEMA sales;
CREATE TABLE sales.orders (order_id text PRIMARY KEY, amount_cents bigint NOT NULL, batch text NOT NULL);
GRANT USAGE ON SCHEMA sales TO orders_reader, other_reader;
GRANT SELECT ON sales.orders TO orders_reader;

ALTER ROLE orders_reader SET client_encoding = 'LATIN1';
CREATE TABLE sales.order_history () INHERITS (sales.orders);
INSERT INTO sales.order_history VALUES ('0007',999,'historical-child');

CREATE TABLE sales.regional_orders (
    region text, order_id text, exact_total numeric, memo text,
    PRIMARY KEY (region,order_id) INCLUDE (memo)
) PARTITION BY LIST (region);
CREATE TABLE sales.regional_east PARTITION OF sales.regional_orders FOR VALUES IN ('east');
CREATE TABLE sales.regional_west PARTITION OF sales.regional_orders FOR VALUES IN ('west');
INSERT INTO sales.regional_orders VALUES ('west','0011',null,null), ('east','0007',12345678901234567890.01230,'订单 · café');
GRANT SELECT ON sales.regional_orders TO orders_reader;
