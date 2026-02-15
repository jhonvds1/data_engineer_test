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

def remove_invalid_orders(data_carts: pd.DataFrame) -> pd.DataFrame:
    data_products = extract_collection('products')

    # explode preservando índice do carrinho
    carts_exploded = data_carts.explode('products').reset_index().rename(columns={'index': 'cart_index'})

    # normaliza produtos
    products = pd.json_normalize(carts_exploded['products'])
    products['cart_index'] = carts_exploded['cart_index'].values

    # dados mínimos do produto
    products_minimum = data_products[['id', 'minimumOrderQuantity']].rename(columns={'id': 'product_id'})

    # produtos do carrinho
    products_cart = products[['cart_index', 'id', 'quantity']].rename(columns={'id': 'product_id'})

    # join para validação
    products_check = pd.merge(products_cart, products_minimum, on='product_id', how='inner')

    # carrinhos inválidos
    invalid_carts = products_check.loc[
        products_check['quantity'] < products_check['minimumOrderQuantity'],
        'cart_index'
    ].unique()


    # filtra carrinhos válidos
    data_carts = data_carts.drop(index=invalid_carts)
    
    return data_carts


def run_etl_carts() -> pd.DataFrame:
    data_carts = extract_collection('carts')
    data_carts = remove_invalid_orders(data_carts)
    data_carts = drop_missing_values(data_carts)
    data_carts = drop_inconsistent_values(data_carts)
    data_carts = transform_transaction_date(data_carts)
    return data_carts


    



