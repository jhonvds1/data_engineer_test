-- Tabela de usuários
CREATE TABLE IF NOT EXISTS dim_users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    age INT,
    gender VARCHAR(20),
    city VARCHAR(20),
    state VARCHAR(20),
    country VARCHAR(20)
);

-- Tabela de produtos
CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,
    title VARCHAR(50),
    price NUMERIC(10,2),
    rating FLOAT,
    brand VARCHAR(20)
);

-- Tabela de tempo corrigida
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

-- Tabela de fatos de vendas
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES dim_users(user_id),
    product_id INT REFERENCES dim_products(product_id),
    time_id INT REFERENCES dim_time(time_id),
    unit_price NUMERIC(10,2),
    quantity INT,
    CONSTRAINT unique_sale UNIQUE (user_id, product_id, time_id)
);
