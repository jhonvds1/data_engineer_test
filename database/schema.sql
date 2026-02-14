CREATE TABLE IF NOT EXISTS dim_users(
	user_id SERIAL PRIMARY KEY,
	first_name VARCHAR(20),
	last_name VARCHAR(20),
	age INT,
	gender VARCHAR(20),
	city VARCHAR(20),
	state VARCHAR(20),
	country VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_products(
	product_id SERIAL PRIMARY KEY,
	title VARCHAR(50),
	price NUMERIC(10,2),
	rating FLOAT,
	brand varchar(20)
);

CREATE TABLE IF NOT EXISTS dim_time(
	time_id SERIAL PRIMARY KEY,
	date DATE,
	year INT,
	month INT,
	day INT
);

CREATE TABLE IF NOT EXISTS fact_sales(
	sale_id SERIAL PRIMARY KEY,
	user_id INT REFERENCES dim_users(user_id),
	product_id INT REFERENCES dim_products(product_id),
	time_id INT REFERENCES dim_time(time_id),
	unit_price NUMERIC(10,2),
	quantity INT,
	total_price NUMERIC(12,2) GENERATED ALWAYS AS (unit_price * quantity) STORED
);

CREATE INDEX idx_fact_user ON fact_sales(user_id);
CREATE INDEX idx_fact_product ON fact_sales(product_id);
CREATE INDEX idx_fact_time ON fact_sales(time_id);

