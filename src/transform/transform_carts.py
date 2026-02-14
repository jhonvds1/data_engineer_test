from ..extract.extract import extract_collection
import pandas as pd


data_carts = extract_collection('carts')

# print(len(data_carts))

# tirar vazios
# data_cart = data_carts.dropna(subset=['userId', 'products', 'total', 'discountedTotal'])

# tratar total price
# data_carts = data_carts[data_carts['total'] >= 0]

# tratar total product
# data_carts = data_carts[data_carts['totalProducts'] >= 0]

# tratar totalQuantity
# data_carts = data_carts[data_carts['totalQuantity'] >= 0]

# TODO: ver se a quantidade de produtos no carrinho é maior q a minima por id de produto

# tratar transaction_date

# def parse_date(date):
#     if isinstance(date, int):
#         return pd.to_datetime(date, unit='s')
#     date = str(date)
#     date = date.replace('T', ' ').replace('Z', '')
#     if '-' in date:
#         return pd.to_datetime(date, errors='coerce')
#     if '/' in date:
#         parts = date.split('/')
#         new_date = parts[2]+'-'+parts[1]+'-'+parts[0]
#         return pd.to_datetime(new_date, errors='coerce')
#     return pd.to_datetime(date)
    

# data_carts['transaction_date'] = data_carts['transaction_date'].apply(parse_date)
# data_carts = data_carts.dropna(subset=['transaction_date'])
# data_carts['transaction_date'] = data_carts['transaction_date'].dt.date

