-- View: Receita total por localização (cidade, estado, país)
CREATE OR REPLACE VIEW vw_revenue_by_location AS 
	SELECT
	    us.city,                                      -- Cidade do usuário
	    us.state,                                     -- Estado do usuário
	    us.country,                                   -- País do usuário
	    SUM(sa.unit_price * sa.quantity) AS revenue_total  -- Soma total de receita por localização
	FROM fact_sales AS sa
	LEFT JOIN dim_users AS us                        -- LEFT JOIN garante que mesmo vendas sem usuário apareçam
	    ON us.user_id = sa.user_id
	GROUP BY us.city, us.state, us.country          -- Agrupa por localização
	ORDER BY revenue_total DESC;                     -- Ordena do maior para o menor valor de receita

-- View: Produtos mais vendidos por receita
CREATE OR REPLACE VIEW vw_top_selling_product AS 
	SELECT 
		pr.product_id,                               -- ID do produto
		pr.title,                                    -- Nome/título do produto
		pr.brand,                                    -- Marca do produto
		SUM(sa.unit_price * sa.quantity) AS revenue  -- Receita total gerada pelo produto
	FROM fact_sales AS sa
	LEFT JOIN dim_products AS pr                  -- LEFT JOIN garante inclusão mesmo se não houver correspondência
		ON sa.product_id = pr.product_id
	GROUP BY pr.product_id, pr.title, pr.brand    -- Agrupa por produto para cálculo de receita
	ORDER BY revenue DESC;                        -- Ordena do maior para o menor valor de receita

-- View: Receita por marca em cada estado
CREATE OR REPLACE VIEW vw_top_brand_selling_state AS
	SELECT
		pr.brand,                                    -- Marca do produto
		us.state,                                    -- Estado do usuário
		SUM(unit_price * quantity) AS revenue        -- Receita total por marca e estado
	FROM fact_sales AS sa
	LEFT JOIN dim_products AS pr
		ON sa.product_id = pr.product_id
	LEFT JOIN dim_users AS us
		ON sa.user_id = us.user_id
	GROUP BY us.state, pr.brand                    -- Agrupa por estado e marca
	ORDER BY revenue DESC;                         -- Ordena do maior para o menor valor de receita

-- View: Vendas e receita por avaliação de produto
CREATE OR REPLACE VIEW vw_rating_sales AS
	SELECT
	    pr.product_id,                               -- ID do produto
	    pr.title,                                    -- Nome/título do produto
	    pr.brand,                                    -- Marca do produto
	    pr.rating,                                   -- Avaliação do produto
	    SUM(sa.quantity) AS total_units_sold,       -- Total de unidades vendidas
	    SUM(sa.unit_price * sa.quantity) AS revenue_total  -- Receita total gerada pelo produto
	FROM fact_sales AS sa
	LEFT JOIN dim_products AS pr
	    ON sa.product_id = pr.product_id
	GROUP BY pr.product_id, pr.title, pr.brand, pr.rating  -- Agrupa também por rating para análise
	ORDER BY total_units_sold DESC;                       -- Ordena pelo número de unidades vendidas

-- View: Receita por mês (nomes dos meses)
CREATE OR REPLACE VIEW vw_top_selling_months AS
	SELECT 
		CASE ti.month                                -- Converte número do mês em nome do mês
	        WHEN 1 THEN 'Janeiro'
	        WHEN 2 THEN 'Fevereiro'
	        WHEN 3 THEN 'Março'
	        WHEN 4 THEN 'Abril'
	        WHEN 5 THEN 'Maio'
	        WHEN 6 THEN 'Junho'
	        WHEN 7 THEN 'Julho'
	        WHEN 8 THEN 'Agosto'
	        WHEN 9 THEN 'Setembro'
	        WHEN 10 THEN 'Outubro'
	        WHEN 11 THEN 'Novembro'
	        WHEN 12 THEN 'Dezembro'
	    END AS month,
		SUM(sa.quantity * sa.unit_price) AS revenue  -- Receita total por mês
	FROM fact_sales AS sa
	LEFT JOIN dim_time AS ti                        -- Relaciona vendas com a dimensão de tempo
		ON sa.time_id = ti.time_id
	GROUP BY ti.month                                -- Agrupa por número do mês
	ORDER BY revenue DESC;                           -- Ordena do mês com maior receita para o menor
