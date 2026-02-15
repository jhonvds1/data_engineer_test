import logging
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from psycopg2.extras import execute_batch

load_dotenv()

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
        raise

def load_dim_users(data_users: pd.DataFrame, cursor):
    logging.info(f"Iniciando carga da dimensão users ({len(data_users)} registros)")
    if data_users.empty:
        logging.warning("DataFrame de usuários vazio")
        return
    execute_batch(cursor, """
        INSERT INTO dim_users (user_id, first_name, last_name, age, gender, city, state, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
    """, data_users[['id', 'firstName', 'lastName', 'age', 'gender', 'city', 'state', 'country']].drop_duplicates().values.tolist())
    logging.info(f"Dim_users concluída, registros inseridos: {cursor.rowcount}")

def load_dim_products(data_products: pd.DataFrame, cursor):
    logging.info(f"Iniciando carga da dimensão products ({len(data_products)} registros)")
    if data_products.empty:
        logging.warning("DataFrame de produtos vazio")
        return
    execute_batch(cursor, """
        INSERT INTO dim_products (product_id, title, price, rating, brand)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """, data_products[['id', 'title', 'price', 'rating', 'brand']].drop_duplicates().values.tolist())
    logging.info(f"Dim_products concluída, registros inseridos: {cursor.rowcount}")

def load_dim_time(data_carts: pd.DataFrame, cursor):
    logging.info("Iniciando carga da dimensão time")
    if data_carts.empty:
        logging.warning("DataFrame de carrinhos vazio")
        return pd.DataFrame(columns=['time_id', 'transaction_date'])
    
    # Converter datas
    try:
        data_carts['transaction_date'] = pd.to_datetime(data_carts['transaction_date'])
    except Exception as e:
        logging.error(f"Erro ao converter datas: {e}")
        raise

    data_carts['ano'] = data_carts['transaction_date'].dt.year
    data_carts['mes'] = data_carts['transaction_date'].dt.month
    data_carts['dia'] = data_carts['transaction_date'].dt.day

    execute_batch(cursor, """
        INSERT INTO dim_time (date, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
    """, data_carts[['transaction_date', 'ano', 'mes', 'dia']].drop_duplicates().values.tolist())
    logging.info(f"Dim_time carregada, registros inseridos: {cursor.rowcount}")

    # Buscar time_id reais
    cursor.execute("SELECT time_id, date FROM dim_time;")
    time_map = pd.DataFrame(cursor.fetchall(), columns=['time_id', 'transaction_date'])
    logging.info(f"Recuperados {len(time_map)} registros de time_id")
    return time_map

def merge_dfs(data_carts: pd.DataFrame, data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando merge de carts e users")
    merged = pd.merge(data_carts, data_users, how='left', left_on='userId', right_on='id')\
        .rename(columns={'id_x': 'cart_id', 'id_y': 'user_id'})
    missing_users = merged['user_id'].isna().sum()
    if missing_users > 0:
        logging.warning(f"{missing_users} carrinhos não possuem usuário correspondente")
    logging.info(f"Merge concluído: {len(merged)} registros")
    return merged

def load_fact_sales(carts_users: pd.DataFrame, cursor, time_df: pd.DataFrame):
    logging.info("Iniciando carga da fact_sales")
    if carts_users.empty:
        logging.warning("DataFrame de vendas vazio")
        return

    # Conversão de datas
    carts_users['transaction_date'] = pd.to_datetime(carts_users['transaction_date'])
    time_df['transaction_date'] = pd.to_datetime(time_df['transaction_date'])

    # Merge para adicionar time_id
    merged = carts_users.merge(time_df, on='transaction_date', how='left')
    missing_time = merged['time_id'].isna().sum()
    if missing_time > 0:
        logging.warning(f"{missing_time} registros não possuem time_id correspondente")

    # Explode produtos
    carts_exploded = merged.explode('products').reset_index(drop=True)
    products_df = pd.json_normalize(carts_exploded['products'])
    products_df['user_id'] = carts_exploded['user_id']
    products_df['time_id'] = carts_exploded['time_id']
    products_df['unit_price'] = products_df.get('unitPrice', 0)
    products_df['quantity'] = products_df['quantity']

    records_to_insert = products_df[['user_id', 'id', 'time_id', 'unit_price', 'quantity']].values.tolist()
    logging.info(f"Total de registros a tentar inserir na fact_sales: {len(records_to_insert)}")

    execute_batch(cursor, """
        INSERT INTO fact_sales (user_id, product_id, time_id, unit_price, quantity)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id, product_id, time_id) DO NOTHING;
    """, records_to_insert)
    logging.info(f"Fact_sales concluída, registros inseridos aproximadamente: {cursor.rowcount}")

def run_load(data_carts: pd.DataFrame, data_products: pd.DataFrame, data_users: pd.DataFrame):
    logging.info("Iniciando ETL completo")
    conn = connect_db()
    cursor = conn.cursor()

    try:
        load_dim_users(data_users, cursor)
        load_dim_products(data_products, cursor)
        time_df = load_dim_time(data_carts, cursor)
        carts_users = merge_dfs(data_carts, data_users)
        load_fact_sales(carts_users, cursor, time_df)
        conn.commit()
        logging.info("Commit realizado com sucesso")
    except Exception as e:
        logging.error(f"Erro durante o ETL: {e}")
        conn.rollback()
        logging.info("Rollback executado devido a erro")
        raise
    finally:
        cursor.close()
        conn.close()
        logging.info("Conexão com PostgreSQL fechada")
