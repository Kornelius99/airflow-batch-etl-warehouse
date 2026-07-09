-- Seeds the source database with a small sample "orders" table so the
-- Airflow DAG has something realistic to extract on first run.

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    region VARCHAR(50),
    product_category VARCHAR(50),
    quantity INT,
    unit_price NUMERIC(10, 2),
    total_amount NUMERIC(10, 2)
);

INSERT INTO orders (customer_id, order_date, region, product_category, quantity, unit_price, total_amount)
VALUES
    (101, '2024-01-01', 'North',  'Electronics', 2, 25.00, 50.00),
    (102, '2024-01-01', 'South',  'Books',       1, 15.00, 15.00),
    (103, '2024-01-02', 'East',   'Electronics', 3, 25.00, 75.00),
    (104, '2024-01-02', 'West',   'Home',        1, 40.00, 40.00),
    (105, '2024-01-03', 'North',  'Books',       4, 15.00, 60.00),
    (106, '2024-01-03', 'South',  'Home',        2, 40.00, 80.00),
    (107, '2024-01-04', 'East',   'Electronics', 1, 25.00, 25.00),
    (108, '2024-01-04', 'West',   'Books',       5, 15.00, 75.00);
