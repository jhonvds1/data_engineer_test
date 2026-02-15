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
                """, data_users[['id', 'firstName', 'lastName', 'age', 'gender', 'city', 'state', 'country']].values.tolist())

def load_dim_products(data_products: pd.DataFrame, cursor):
    execute_batch(cursor, """
                    INSERT INTO dim_products (product_id, title, price, rating, brand)
                    VALUES (%s, %s, %s, %s, %s);
                """, data_products[['id', 'title', 'price', 'rating', 'brand']].values.tolist())

def load_dim_time(data_carts: pd.DataFrame, cursor):
    execute_batch(cursor, """
                    INSERT INTO dim_time (time_id, date, year, month, day)
                    VALUES (%s, %s, %s, %s, %s);
                  """, ...)

def load_fact_sales(data_carts: pd.DataFrame, cursor):
    execute_batch(cursor, """
                    INSERT INTO fact_sales (sale_id, user_id, product_id, time_id, unit_price, quantity)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, ...)


def run_load(data_carts:pd.DataFrame, data_products: pd.DataFrame, data_users: pd.DataFrame):
    conn = connect_db()
    cursor = conn.cursor()

    load_dim_users(data_users, cursor)
    
    load_dim_products(data_products, cursor)
    
    load_fact_sales(data_carts, cursor)
    
    conn.commit()
    cursor.close()
    conn.close()
