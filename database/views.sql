CREATE OR REPLACE VIEW vw_revenue_by_location AS 
	SELECT
	    us.city,
	    us.state,
	    us.country,
    SUM(sa.unit_price * sa.quantity) AS revenue_total
	FROM fact_sales AS sa
	LEFT JOIN dim_users AS us
	    ON us.user_id = sa.user_id
	GROUP BY us.city, us.state, us.country
	ORDER BY revenue_total DESC;

CREATE OR REPLACE VIEW vw_top_selling_product AS 
	SELECT 
		pr.product_id,
		pr.title,
		pr.brand,
		SUM(sa.unit_price * sa.quantity) AS revenue
	FROM fact_sales AS sa
	LEFT JOIN dim_products AS pr
		ON sa.product_id = pr.product_id
	GROUP BY pr.product_id, pr.title, pr.brand
	ORDER BY revenue DESC;

CREATE OR REPLACE VIEW vw_top_brand_selling_state AS
	SELECT
		pr.brand,
		us.state,
		SUM(unit_price * quantity) AS revenue
	FROM fact_sales AS sa
	LEFT JOIN dim_products AS pr
		ON sa.product_id = pr.product_id
	LEFT JOIN dim_users AS us
		ON sa.user_id = us.user_id
	GROUP BY us.state, pr.brand
	ORDER BY revenue DESC;

CREATE OR REPLACE VIEW vw_rating_sales AS
	SELECT
	    pr.product_id,
	    pr.title,
	    pr.brand,
	    pr.rating,
    SUM(sa.quantity) AS total_units_sold,
    SUM(sa.unit_price * sa.quantity) AS revenue_total
	FROM fact_sales AS sa
	LEFT JOIN dim_products AS pr
	    ON sa.product_id = pr.product_id
	GROUP BY pr.product_id, pr.title, pr.brand, pr.rating
	ORDER BY total_units_sold DESC;

CREATE OR REPLACE VIEW vw_top_selling_months AS
	SELECT 
		CASE ti.month
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
		SUM(sa.quantity * sa.unit_price) AS revenue
	FROM fact_sales AS sa
	LEFT JOIN dim_time AS ti
		ON sa.time_id = ti.time_id
	GROUP BY ti.month
	ORDER BY revenue DESC