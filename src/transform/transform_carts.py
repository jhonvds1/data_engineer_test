import logging
import pandas as pd
from ..extract.extract import extract_collection

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def drop_missing_values(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores ausentes (userId, products, total, discountedTotal)")
    before = len(data_carts)
    data_carts = data_carts.dropna(subset=['userId', 'products', 'total', 'discountedTotal'])
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por valores ausentes")
    logging.info(f"Registros restantes: {len(data_carts)}")
    return data_carts

def drop_inconsistent_values(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores inconsistentes (total, totalProducts, totalQuantity negativos)")
    before = len(data_carts)
    data_carts = data_carts[data_carts['total'] >= 0]
    data_carts = data_carts[data_carts['totalProducts'] >= 0]
    data_carts = data_carts[data_carts['totalQuantity'] >= 0]
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por inconsistências")
    logging.info(f"Registros restantes: {len(data_carts)}")
    return data_carts

def parse_date(date) -> pd.Timestamp:
    try:
        if isinstance(date, int):
            return pd.to_datetime(date, unit='s')
        date = str(date)
        date = date.replace('T', ' ').replace('Z', '')
        if '-' in date:
            return pd.to_datetime(date, errors='coerce')
        if '/' in date:
            parts = date.split('/')
            new_date = parts[2] + '-' + parts[1] + '-' + parts[0]
            return pd.to_datetime(new_date, errors='coerce')
        return pd.to_datetime(date)
    except Exception as e:
        logging.warning(f"Falha ao converter data: {date} | Erro: {e}")
        return pd.NaT

def transform_transaction_date(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transformando coluna transaction_date")
    data_carts['transaction_date'] = data_carts['transaction_date'].apply(parse_date)
    before = len(data_carts)
    data_carts = data_carts.dropna(subset=['transaction_date'])
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por data inválida")
    data_carts['transaction_date'] = data_carts['transaction_date'].dt.date
    logging.info(f"Transaction_date transformada com sucesso. Total registros válidos: {len(data_carts)}")
    return data_carts

def remove_invalid_orders(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo pedidos com quantidade de produtos abaixo do mínimo")
    data_products = extract_collection('products')
    before = len(data_carts)

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
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} carrinhos removidos por não atenderem quantidade mínima")
    logging.info(f"Carrinhos válidos restantes: {len(data_carts)}")
    return data_carts

def run_etl_carts() -> pd.DataFrame:
    logging.info("Iniciando ETL de carts")
    try:
        data_carts = extract_collection('carts')
        logging.info(f"{len(data_carts)} registros extraídos da coleção 'carts'")

        data_carts = remove_invalid_orders(data_carts)
        data_carts = drop_missing_values(data_carts)
        data_carts = drop_inconsistent_values(data_carts)
        data_carts = transform_transaction_date(data_carts)

        logging.info(f"ETL de carts concluído com {len(data_carts)} registros válidos")
        return data_carts
    except Exception as e:
        logging.error(f"Falha no ETL de carts: {e}")
        raise
