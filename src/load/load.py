import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from psycopg2.extras import execute_batch


load_dotenv()

def connect_db():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def load_dim_users(data_users: pd.DataFrame, cursor):
    execute_batch(cursor, """
        INSERT INTO dim_users (user_id, first_name, last_name, age, gender, city, state, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
    """, data_users[['id', 'firstName', 'lastName', 'age', 'gender', 'city', 'state', 'country']].drop_duplicates().values.tolist())


def load_dim_products(data_products: pd.DataFrame, cursor):
    execute_batch(cursor, """
        INSERT INTO dim_products (product_id, title, price, rating, brand)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """, data_products[['id', 'title', 'price', 'rating', 'brand']].drop_duplicates().values.tolist())


def load_dim_time(data_carts: pd.DataFrame, cursor):
    data_carts['transaction_date'] = pd.to_datetime(data_carts['transaction_date'])
    data_carts['ano'] = data_carts['transaction_date'].dt.year
    data_carts['mes'] = data_carts['transaction_date'].dt.month
    data_carts['dia'] = data_carts['transaction_date'].dt.day

    # Insere novas datas (se houver)
    execute_batch(cursor, """
        INSERT INTO dim_time (date, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
    """, data_carts[['transaction_date', 'ano', 'mes', 'dia']].drop_duplicates().values.tolist())

    # Agora busca os time_id reais da tabela
    cursor.execute("SELECT time_id, date FROM dim_time;")
    time_map = pd.DataFrame(cursor.fetchall(), columns=['time_id', 'transaction_date'])

    # Retorna o mapeamento correto
    return time_map

def load_fact_sales(carts_users: pd.DataFrame, cursor, time_df: pd.DataFrame):
    # Converter transaction_date para datetime nos dois DataFrames
    carts_users['transaction_date'] = pd.to_datetime(carts_users['transaction_date'])
    time_df['transaction_date'] = pd.to_datetime(time_df['transaction_date'])

    # Merge com tipos iguais
    carts_users = carts_users.merge(time_df, on='transaction_date', how='left')

    # Explode a coluna de produtos
    carts_exploded = carts_users.explode('products').reset_index(drop=True)
    products_df = pd.json_normalize(carts_exploded['products'])
    products_df['user_id'] = carts_exploded['userId']
    products_df['time_id'] = carts_exploded['time_id']
    products_df['unit_price'] = products_df.get('unitPrice', 0)
    products_df['quantity'] = products_df['quantity']

    records_to_insert = products_df[['user_id', 'id', 'time_id', 'unit_price', 'quantity']].values.tolist()

    execute_batch(cursor, """
        INSERT INTO fact_sales (user_id, product_id, time_id, unit_price, quantity)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id, product_id, time_id) DO NOTHING;
    """, records_to_insert)

def merge_dfs(data_carts:pd.DataFrame, data_users: pd.DataFrame) -> pd.DataFrame:
    carts_users = pd.merge(data_carts, data_users, how='left', left_on='userId', right_on='id').rename(columns={'id_x' : 'cart_id', 'id_y' : 'user_id'})
    return carts_users


def run_load(data_carts:pd.DataFrame, data_products: pd.DataFrame, data_users: pd.DataFrame):
    conn = connect_db()
    cursor = conn.cursor()

    load_dim_users(data_users, cursor)
    
    load_dim_products(data_products, cursor)
    
    time_df = load_dim_time(data_carts, cursor)

    carts_users = merge_dfs(data_carts, data_users)

    load_fact_sales(carts_users, cursor, time_df)
    
    conn.commit()
    cursor.close()
    conn.close()
