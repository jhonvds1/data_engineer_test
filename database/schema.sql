-- Tabela de usuários (dimensão de usuários)
CREATE TABLE IF NOT EXISTS dim_users (
    user_id SERIAL PRIMARY KEY,        -- Identificador único do usuário, auto-incrementado
    first_name VARCHAR(20),            -- Primeiro nome do usuário (até 20 caracteres)
    last_name VARCHAR(20),             -- Sobrenome do usuário (até 20 caracteres)
    age INT,                           -- Idade do usuário
    gender VARCHAR(20),                -- Gênero do usuário (até 20 caracteres; poderia ser ENUM para padronização)
    city VARCHAR(20),                  -- Cidade do usuário
    state VARCHAR(20),                 -- Estado do usuário
    country VARCHAR(20)                -- País do usuário
);

-- Tabela de produtos (dimensão de produtos)
CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,     -- Identificador único do produto, auto-incrementado
    title VARCHAR(50),                 -- Nome/título do produto
    price NUMERIC(10,2),               -- Preço do produto com precisão decimal (até 99999999.99)
    rating FLOAT,                      -- Avaliação do produto (permitindo casas decimais)
    brand VARCHAR(20)                  -- Marca do produto
);

-- Tabela de tempo (dimensão de tempo)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,        -- Identificador único da linha de tempo
    date DATE UNIQUE,                  -- Data real, única para garantir integridade
    year INT,                          -- Ano da data
    month INT,                         -- Mês da data
    day INT                            -- Dia da data
);

-- Tabela de fatos de vendas (fact table)
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,        -- Identificador único da venda, auto-incrementado
    user_id INT REFERENCES dim_users(user_id),          -- Chave estrangeira para usuário
    product_id INT REFERENCES dim_products(product_id), -- Chave estrangeira para produto
    time_id INT REFERENCES dim_time(time_id),           -- Chave estrangeira para tempo
    unit_price NUMERIC(10,2),          -- Preço unitário do produto na venda (captura variações, promoções)
    quantity INT,                       -- Quantidade vendida
    CONSTRAINT unique_sale UNIQUE (user_id, product_id, time_id)  -- Garante que não haja duplicidade de vendas do mesmo usuário, produto e data
);
