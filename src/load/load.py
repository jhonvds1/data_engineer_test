import logging
import psycopg2                       # Biblioteca para conexão e execução de SQL no PostgreSQL
from dotenv import load_dotenv        # Para carregar variáveis de ambiente de um arquivo .env
import os                             # Para acessar variáveis de ambiente
import pandas as pd                   # Para manipulação de dados em DataFrames
from psycopg2.extras import execute_values  # Função eficiente para inserção em lote no PostgreSQL

# Carrega variáveis do arquivo .env
load_dotenv()

# Configuração de logs para acompanhamento do ETL
logging.basicConfig(
    level=logging.INFO,               # Nível INFO (inclui INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato: timestamp - nível - mensagem
)

# Função para conectar ao PostgreSQL
def connect_db():
    """Conecta ao PostgreSQL e retorna a conexão"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        logging.info("Conexão com PostgreSQL estabelecida")
        return conn
    except Exception as e:
        logging.error(f"Falha ao conectar no PostgreSQL: {e}")
        raise  # Propaga o erro para o chamador tratar

# Função para criar todas as tabelas do modelo estrela
def create_tables(cursor):
    tables = {
        "dim_users": """
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
        """,
        "dim_products": """
            CREATE TABLE IF NOT EXISTS dim_products (
                product_id SERIAL PRIMARY KEY,
                title VARCHAR(50),
                price NUMERIC(10,2),
                rating FLOAT,
                brand VARCHAR(20)
            );
        """,
        "dim_time": """
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                year INT,
                month INT,
                day INT
            );
        """,
        "fact_sales": """
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_id SERIAL PRIMARY KEY,
                user_id INT REFERENCES dim_users(user_id),
                product_id INT REFERENCES dim_products(product_id),
                time_id INT REFERENCES dim_time(time_id),
                unit_price NUMERIC(10,2),
                quantity INT,
                CONSTRAINT unique_sale UNIQUE (user_id, product_id, time_id)
            );
        """
    }

    # Itera sobre todas as tabelas definidas e cria no banco
    for table_name, sql in tables.items():
        try:
            print(f"[INFO] Criando tabela '{table_name}'...")
            cursor.execute(sql)
            print(f"[SUCCESS] Tabela '{table_name}' criada ou já existia.")
        except Exception as e:
            print(f"[ERROR] Falha ao criar tabela '{table_name}': {e}")

# Função para criar todas as views definidas
def create_views(cursor):
    views = {
        "vw_revenue_by_location": """
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
        """,
        "vw_top_selling_product": """
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
        """,
        "vw_top_brand_selling_state": """
            CREATE OR REPLACE VIEW vw_top_brand_selling_state AS
            SELECT
                pr.brand,
                us.state,
                SUM(sa.unit_price * sa.quantity) AS revenue
            FROM fact_sales AS sa
            LEFT JOIN dim_products AS pr
                ON sa.product_id = pr.product_id
            LEFT JOIN dim_users AS us
                ON sa.user_id = us.user_id
            GROUP BY us.state, pr.brand
            ORDER BY revenue DESC;
        """,
        "vw_rating_sales": """
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
        """,
        "vw_top_selling_months": """
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
            ORDER BY revenue DESC;
        """
    }

    # Criação das views iterativamente
    for view_name, sql in views.items():
        try:
            print(f"[INFO] Criando view '{view_name}'...")
            cursor.execute(sql)
            print(f"[SUCCESS] View '{view_name}' criada ou atualizada com sucesso.")
        except Exception as e:
            print(f"[ERROR] Falha ao criar view '{view_name}': {e}")

# Função para carregar dimensão de usuários
def load_dim_users(data_users: pd.DataFrame, cursor):
    logging.info(f"Iniciando carga da dimensão users ({len(data_users)} registros)")
    if data_users.empty:
        logging.warning("DataFrame de usuários vazio")
        return
    execute_values(cursor, """
        INSERT INTO dim_users (user_id, first_name, last_name, age, gender, city, state, country)
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING;
    """, data_users[['id', 'firstName', 'lastName', 'age', 'gender', 'city', 'state', 'country']].drop_duplicates().values.tolist())
    logging.info(f"Dim_users concluída, registros inseridos: {cursor.rowcount}")

# Função para carregar dimensão de produtos
def load_dim_products(data_products: pd.DataFrame, cursor):
    logging.info(f"Iniciando carga da dimensão products ({len(data_products)} registros)")
    if data_products.empty:
        logging.warning("DataFrame de produtos vazio")
        return
    execute_values(cursor, """
        INSERT INTO dim_products (product_id, title, price, rating, brand)
        VALUES %s
        ON CONFLICT (product_id) DO NOTHING;
    """, data_products[['id', 'title', 'price', 'rating', 'brand']].drop_duplicates().values.tolist())
    logging.info(f"Dim_products concluída, registros inseridos: {cursor.rowcount}")

# Função para carregar dimensão de tempo
def load_dim_time(data_carts: pd.DataFrame, cursor):
    logging.info("Iniciando carga da dimensão time")
    if data_carts.empty:
        logging.warning("DataFrame de carrinhos vazio")
        return pd.DataFrame(columns=['time_id', 'transaction_date'])
    
    # Converter coluna de datas para datetime
    try:
        data_carts['transaction_date'] = pd.to_datetime(data_carts['transaction_date'])
    except Exception as e:
        logging.error(f"Erro ao converter datas: {e}")
        raise

    # Extrair ano, mês e dia
    data_carts['ano'] = data_carts['transaction_date'].dt.year
    data_carts['mes'] = data_carts['transaction_date'].dt.month
    data_carts['dia'] = data_carts['transaction_date'].dt.day

    # Inserção em lote com ON CONFLICT para evitar duplicidade
    execute_values(cursor, """
        INSERT INTO dim_time (date, year, month, day)
        VALUES %s
        ON CONFLICT (date) DO NOTHING;
    """, data_carts[['transaction_date', 'ano', 'mes', 'dia']].drop_duplicates().values.tolist())
    logging.info(f"Dim_time carregada, registros inseridos: {cursor.rowcount}")

    # Recupera time_id gerados no banco
    cursor.execute("SELECT time_id, date FROM dim_time;")
    time_map = pd.DataFrame(cursor.fetchall(), columns=['time_id', 'transaction_date'])
    logging.info(f"Recuperados {len(time_map)} registros de time_id")
    return time_map

# Função para fazer merge de carts com usuários
def merge_dfs(data_carts: pd.DataFrame, data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando merge de carts e users")
    merged = pd.merge(data_carts, data_users, how='left', left_on='userId', right_on='id')\
        .rename(columns={'id_x': 'cart_id', 'id_y': 'user_id'})
    missing_users = merged['user_id'].isna().sum()
    if missing_users > 0:
        logging.warning(f"{missing_users} carrinhos não possuem usuário correspondente")
    logging.info(f"Merge concluído: {len(merged)} registros")
    return merged

# Função para carregar tabela de fatos de vendas
def load_fact_sales(carts_users: pd.DataFrame, cursor, time_df: pd.DataFrame):
    logging.info("Iniciando carga da fact_sales")
    if carts_users.empty:
        logging.warning("DataFrame de vendas vazio")
        return

    # Conversão de datas para merge com dimensão de tempo
    carts_users['transaction_date'] = pd.to_datetime(carts_users['transaction_date'])
    time_df['transaction_date'] = pd.to_datetime(time_df['transaction_date'])

    # Merge para adicionar time_id
    merged = carts_users.merge(time_df, on='transaction_date', how='left')
    missing_time = merged['time_id'].isna().sum()
    if missing_time > 0:
        logging.warning(f"{missing_time} registros não possuem time_id correspondente")

    # Explode lista de produtos em linhas separadas
    carts_exploded = merged.explode('products').reset_index(drop=True)
    products_df = pd.json_normalize(carts_exploded['products'])  # Normaliza dict de produtos
    products_df['user_id'] = carts_exploded['user_id']
    products_df['time_id'] = carts_exploded['time_id']
    products_df['price'] = products_df.get('price', 0)
    products_df['quantity'] = products_df['quantity']

    # Prepara registros para inserção
    records_to_insert = products_df[['user_id', 'id', 'time_id', 'price', 'quantity']].values.tolist()
    logging.info(f"Total de registros a tentar inserir na fact_sales: {len(records_to_insert)}")

    # Inserção em lote na fact_sales
    execute_values(cursor, """
        INSERT INTO fact_sales (user_id, product_id, time_id, unit_price, quantity)
        VALUES %s
        ON CONFLICT (user_id, product_id, time_id) DO NOTHING;
    """, records_to_insert)
    logging.info(f"Fact_sales concluída, registros inseridos aproximadamente: {cursor.rowcount}")

import pandas as pd

# Exibe todas as views existentes no banco de dados
def show_views(cursor):
    try:
        # Consulta do information_schema para listar views
        query = """
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
        """

        # Executa a query
        cursor.execute(query)
        result = cursor.fetchall()

        # Converte para DataFrame para visualização
        df_views = pd.DataFrame(result, columns=['schema', 'view_name'])

        # Exibe o DataFrame
        print("Views disponíveis no banco:")
        print(df_views)

        return df_views

    except Exception as e:
        print(f"Erro ao listar views: {e}")
        return pd.DataFrame(columns=['schema', 'view_name'])


# Função principal para rodar todo o ETL
def run_load(data_carts: pd.DataFrame, data_products: pd.DataFrame, data_users: pd.DataFrame):
    logging.info("Iniciando ETL completo")
    conn = connect_db()
    cursor = conn.cursor()

    try:
        create_tables(cursor)                              # Cria todas as tabelas
        load_dim_users(data_users, cursor)                 # Carrega dimensão users
        load_dim_products(data_products, cursor)           # Carrega dimensão products
        time_df = load_dim_time(data_carts, cursor)        # Carrega dimensão time
        carts_users = merge_dfs(data_carts, data_users)    # Faz merge de carrinhos e usuários
        load_fact_sales(carts_users, cursor, time_df)      # Carrega tabela de fatos
        create_views(cursor)                               # Cria as views de análise
        conn.commit()                                      # Confirma todas as alterações no banco
        show_views(cursor)                                 # Exibe as Views criadas
        logging.info("Commit realizado com sucesso")
    except Exception as e:
        logging.error(f"Erro durante o ETL: {e}")
        conn.rollback()                                    # Reverte alterações em caso de erro
        logging.info("Rollback executado devido a erro")
        raise
    finally:
        cursor.close()
        conn.close()
        logging.info("Conexão com PostgreSQL fechada")
