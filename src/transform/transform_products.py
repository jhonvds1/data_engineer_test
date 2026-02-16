import logging
import pandas as pd
# Importa função de extração da coleção MongoDB
from ..extract.extract import extract_collection

# Configuração global de logs para o ETL de products
logging.basicConfig(
    level=logging.INFO,  # Nível de log INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Remove registros com valores ausentes obrigatórios
def drop_missing_values(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores ausentes (title, price)")
    before = len(data_products)
    # Remove linhas com NaN em colunas essenciais
    data_products = data_products.dropna(subset=['title', 'price'])
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por valores ausentes")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

# Remove registros duplicados com base em campos críticos
def drop_duplicates_values(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros duplicados (title, sku)")
    before = len(data_products)
    unique_fields = ['title', 'sku']
    # Mantém a primeira ocorrência e remove duplicados
    data_products = data_products.drop_duplicates(subset=unique_fields, keep='first')
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros duplicados removidos")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

# Remove registros com campos vazios ou apenas espaços
def drop_spaces(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com campos vazios ou apenas espaços")
    before = len(data_products)
    # Colunas críticas que não podem estar vazias
    cols = ['title','description','category','brand','sku',
            'warrantyInformation','shippingInformation',
            'availabilityStatus','returnPolicy','thumbnail']
    # Cria máscara que verifica se todos os campos têm conteúdo válido
    mask = data_products[cols].apply(lambda x: x.str.strip() != '').all(axis=1)
    data_products = data_products[mask]
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por campos vazios ou espaços")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

# Remove registros com valores inconsistentes (negativos ou fora de faixa)
def drop_inconsistent_values(data_products: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores inconsistentes")
    before = len(data_products)
    mask = (
        (data_products['price'] >= 0) &  # Preço não pode ser negativo
        (data_products['discountPercentage'] >= 0) & (data_products['discountPercentage'] <= 100) &  # Desconto entre 0 e 100%
        (data_products['rating'] >= 0) & (data_products['rating'] <= 5) &  # Nota entre 0 e 5
        (data_products['stock'] >= 0) &  # Estoque não negativo
        (data_products['weight'] >= 0) &  # Peso não negativo
        (data_products['minimumOrderQuantity'] >= 0)  # Quantidade mínima não negativa
    )
    data_products = data_products[mask]
    removed = before - len(data_products)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por inconsistências")
    logging.info(f"Registros restantes: {len(data_products)}")
    return data_products

# Função principal do ETL de products
def run_etl_products() -> pd.DataFrame:
    logging.info("Iniciando ETL de products")
    try:
        # Extração dos dados da coleção MongoDB 'products'
        data_products = extract_collection('products')
        logging.info(f"{len(data_products)} registros extraídos da coleção 'products'")

        # Sequência de limpeza e transformação
        data_products = drop_missing_values(data_products)
        data_products = drop_duplicates_values(data_products)
        data_products = drop_spaces(data_products)
        data_products = drop_inconsistent_values(data_products)

        logging.info(f"ETL de products concluído com {len(data_products)} registros válidos")
        return data_products
    except Exception as e:
        logging.error(f"Falha no ETL de products: {e}")
        raise
