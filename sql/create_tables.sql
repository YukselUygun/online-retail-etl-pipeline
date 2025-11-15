CREATE TABLE IF NOT EXISTS stg_retail (
    invoice_no TEXT,
    stock_code TEXT,
    description TEXT,
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10,2),
    customer_id INT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS retail_clean (
    invoice_no TEXT,
    stock_code TEXT,
    description TEXT,
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10,2),
    customer_id INT,
    country TEXT,
    total_price NUMERIC(12,2),
    is_cancelled BOOLEAN,
    customer_valid BOOLEAN
);
