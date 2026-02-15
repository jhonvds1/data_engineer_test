import logging
import pandas as pd
from ..extract.extract import extract_collection

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def drop_missing_values(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores ausentes (title, price)")
    before = len(data_products)
    data_products = data_products.dropna(subset=['title', 'price'])
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por valores ausentes")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

def drop_duplicates_values(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros duplicados (title, sku)")
    before = len(data_products)
    unique_fields = ['title', 'sku']
    data_products = data_products.drop_duplicates(subset=unique_fields, keep='first')
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros duplicados removidos")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

def drop_spaces(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com campos vazios ou apenas espaços")
    before = len(data_products)
    cols = ['title','description','category','brand','sku',
            'warrantyInformation','shippingInformation',
            'availabilityStatus','returnPolicy','thumbnail']
    mask = data_products[cols].apply(lambda x: x.str.strip() != '').all(axis=1)
    data_products = data_products[mask]
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por campos vazios ou espaços")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

def drop_inconsistent_values(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores inconsistentes")
    before = len(data_products)
    mask = (
        (data_products['price'] >= 0) &
        (data_products['discountPercentage'] >= 0) & (data_products['discountPercentage'] <= 100) &
        (data_products['rating'] >= 0) & (data_products['rating'] <= 5) &
        (data_products['stock'] >= 0) &
        (data_products['weight'] >= 0) &
        (data_products['minimumOrderQuantity'] >= 0)
    )
    data_products = data_products[mask]
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por inconsistências")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

def run_etl_products() -> pd.DataFrame:
    logging.info("Iniciando ETL de products")
    try:
        data_products = extract_collection('products')
        logging.info(f"{len(data_products)} registros extraídos da coleção 'products'")

        data_products = drop_missing_values(data_products)
        data_products = drop_duplicates_values(data_products)
        data_products = drop_spaces(data_products)
        data_products = drop_inconsistent_values(data_products)

        logging.info(f"ETL de products concluído com {len(data_products)} registros válidos")
        return data_products
    except Exception as e:
        logging.error(f"Falha no ETL de products: {e}")
        raise
