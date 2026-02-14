from ..extract.extract import extract_collection
import pandas as pd


def drop_missing_values(data_carts: pd.DataFrame) -> pd.DataFrame:
    return data_carts.dropna(subset=['userId', 'products', 'total', 'discountedTotal'])

def drop_inconsistent_values(data_carts: pd.DataFrame) -> pd.DataFrame:
    data_carts = data_carts[data_carts['total'] >= 0]
    data_carts = data_carts[data_carts['totalProducts'] >= 0]
    return data_carts[data_carts['totalQuantity'] >= 0]

def parse_date(date) -> pd.Timestamp:
    if isinstance(date, int):
        return pd.to_datetime(date, unit='s')
    date = str(date)
    date = date.replace('T', ' ').replace('Z', '')
    if '-' in date:
        return pd.to_datetime(date, errors='coerce')
    if '/' in date:
        parts = date.split('/')
        new_date = parts[2]+'-'+parts[1]+'-'+parts[0]
        return pd.to_datetime(new_date, errors='coerce')
    return pd.to_datetime(date)

def transform_transaction_date(data_carts: pd.DataFrame) -> pd.DataFrame:
    data_carts['transaction_date'] = data_carts['transaction_date'].apply(parse_date)
    data_carts = data_carts.dropna(subset=['transaction_date'])
    data_carts['transaction_date'] = data_carts['transaction_date'].dt.date
    return data_carts


def run_etl_carts() -> pd.DataFrame:
    data_carts = extract_collection('carts')
    data_carts = drop_missing_values(data_carts)
    data_carts = drop_inconsistent_values(data_carts)
    data_carts = transform_transaction_date(data_carts)
    return data_carts

# TODO: ver se a quantidade de produtos no carrinho é maior q a minima por id de produto



    



