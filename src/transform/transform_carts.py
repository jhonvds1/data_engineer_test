import logging
import pandas as pd
# Importa função de extração da coleção MongoDB
from ..extract.extract import extract_collection

# Configuração global de logs para o ETL de carts
logging.basicConfig(
    level=logging.INFO,  # Nível de log INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Função para remover registros com valores ausentes críticos
def drop_missing_values(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores ausentes (userId, products, total, discountedTotal)")
    before = len(data_carts)
    # Remove linhas com NaN em colunas críticas
    data_carts = data_carts.dropna(subset=['userId', 'products', 'total', 'discountedTotal'])
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por valores ausentes")
    logging.info(f"Registros restantes: {len(data_carts)}")
    return data_carts

# Função para remover registros com valores inconsistentes
def drop_inconsistent_values(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores inconsistentes (total, totalProducts, totalQuantity negativos)")
    before = len(data_carts)
    # Filtra registros com valores não-negativos
    data_carts = data_carts[data_carts['total'] >= 0]
    data_carts = data_carts[data_carts['totalProducts'] >= 0]
    data_carts = data_carts[data_carts['totalQuantity'] >= 0]
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por inconsistências")
    logging.info(f"Registros restantes: {len(data_carts)}")
    return data_carts

# Função auxiliar para converter datas em diferentes formatos
def parse_date(date) -> pd.Timestamp:
    try:
        if isinstance(date, int):  # Timestamp UNIX
            return pd.to_datetime(date, unit='s')
        date = str(date)
        date = date.replace('T', ' ').replace('Z', '')  # Formato ISO
        if '-' in date:
            return pd.to_datetime(date, errors='coerce')  # Datas já no padrão YYYY-MM-DD
        if '/' in date:  # Datas no formato DD/MM/YYYY
            parts = date.split('/')
            new_date = parts[2] + '-' + parts[1] + '-' + parts[0]
            return pd.to_datetime(new_date, errors='coerce')
        return pd.to_datetime(date)
    except Exception as e:
        logging.warning(f"Falha ao converter data: {date} | Erro: {e}")
        return pd.NaT

# Função para transformar a coluna de datas das transações
def transform_transaction_date(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transformando coluna transaction_date")
    # Aplica função de parse para cada registro
    data_carts['transaction_date'] = data_carts['transaction_date'].apply(parse_date)
    before = len(data_carts)
    # Remove registros com datas inválidas
    data_carts = data_carts.dropna(subset=['transaction_date'])
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por data inválida")
    # Converte para objeto date (sem horário)
    data_carts['transaction_date'] = data_carts['transaction_date'].dt.date
    logging.info(f"Transaction_date transformada com sucesso. Total registros válidos: {len(data_carts)}")
    return data_carts

# Função para remover pedidos que não atendem à quantidade mínima
def remove_invalid_orders(data_carts: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo pedidos com quantidade de produtos abaixo do mínimo")
    data_products = extract_collection('products')  # Extrai dados de produtos
    before = len(data_carts)

    # Explode lista de produtos em linhas individuais, mantendo índice do carrinho
    carts_exploded = data_carts.explode('products').reset_index().rename(columns={'index': 'cart_index'})

    # Normaliza dict de produtos em colunas
    products = pd.json_normalize(carts_exploded['products'])
    products['cart_index'] = carts_exploded['cart_index'].values

    # Obtém quantidade mínima de cada produto
    products_minimum = data_products[['id', 'minimumOrderQuantity']].rename(columns={'id': 'product_id'})

    # Seleciona colunas relevantes do carrinho
    products_cart = products[['cart_index', 'id', 'quantity']].rename(columns={'id': 'product_id'})

    # Validação: merge produtos do carrinho com quantidade mínima
    products_check = pd.merge(products_cart, products_minimum, on='product_id', how='inner')

    # Identifica carrinhos inválidos
    invalid_carts = products_check.loc[
        products_check['quantity'] < products_check['minimumOrderQuantity'],
        'cart_index'
    ].unique()

    # Remove carrinhos inválidos
    data_carts = data_carts.drop(index=invalid_carts)
    removed = before - len(data_carts)
    if removed > 0:
        logging.warning(f"{removed} carrinhos removidos por não atenderem quantidade mínima")
    logging.info(f"Carrinhos válidos restantes: {len(data_carts)}")
    return data_carts

# Função principal do ETL de carts
def run_etl_carts() -> pd.DataFrame:
    logging.info("Iniciando ETL de carts")
    try:
        data_carts = extract_collection('carts')  # Extração da coleção MongoDB
        logging.info(f"{len(data_carts)} registros extraídos da coleção 'carts'")

        # Limpeza e transformação em sequência
        data_carts = remove_invalid_orders(data_carts)
        data_carts = drop_missing_values(data_carts)
        data_carts = drop_inconsistent_values(data_carts)
        data_carts = transform_transaction_date(data_carts)

        logging.info(f"ETL de carts concluído com {len(data_carts)} registros válidos")
        return data_carts
    except Exception as e:
        logging.error(f"Falha no ETL de carts: {e}")
        raise
