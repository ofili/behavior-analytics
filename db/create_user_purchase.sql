CREATE SCHAME retail;

CREATE TABLE retail.purchase (
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(8,2),
    customer_id INTEGER,
    country VARCHAR(20)
);

COPY retail.purchase(invoice_number, stock_code, detail, quantity, invoice_date, unit_price, customer_id, country)
FROM 'input_data/OnlineRetail.csv' DELIMITER ',' CSV HEADER;