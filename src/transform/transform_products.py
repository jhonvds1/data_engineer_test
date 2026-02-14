from ..extract.extract import extract_collection
import pandas as pd


def drop_missing_values(data_products: pd.DataFrame) -> pd.DataFrame:
    return data_products.dropna(subset=['title', 'price'])


def drop_duplicates_values(data_products: pd.DataFrame) -> pd.DataFrame:
    unique_fields = ['title', 'sku']
    return data_products.drop_duplicates(subset=unique_fields, keep='first')


def drop_spaces(data_products: pd.DataFrame) -> pd.DataFrame:
    cols = ['title','description','category','brand','sku',
            'warrantyInformation','shippingInformation',
            'availabilityStatus','returnPolicy','thumbnail']
    mask = data_products[cols].apply(lambda x: x.str.strip() != '').all(axis=1)
    return data_products[mask]

def drop_inconsistent_values(data_products: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (data_products['price'] >= 0) &
        (data_products['discountPercentage'] >= 0) & (data_products['discountPercentage'] <= 100) &
        (data_products['rating'] >= 0) & (data_products['rating'] <= 5) &
        (data_products['stock'] >= 0) &
        (data_products['weight'] >= 0) &
        (data_products['minimumOrderQuantity'] >= 0)
    )
    return data_products[mask]

def run_etl_products() -> pd.DataFrame:
    data_products = extract_collection('products')
    data_products = drop_missing_values(data_products)
    data_products = drop_duplicates_values(data_products)
    data_products = drop_spaces(data_products)
    data_products = drop_inconsistent_values(data_products)
    return data_products