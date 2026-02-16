import logging
import pandas as pd
# Importa funções de transformação específicas de cada entidade
from .transform.transform_carts import run_etl_carts
from .transform.transform_products import run_etl_products
from .transform.transform_users import run_etl_users
# Importa função de carga para PostgreSQL
from .load.load import run_load, connect_db

# Configuração global de logs para todo o pipeline
logging.basicConfig(
    level=logging.INFO,  # Nível de log INFO (inclui INFO, WARNING e ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato com timestamp, nível e mensagem
)

def main() -> pd.DataFrame:
    """
    Função principal que executa o pipeline ETL completo:
    1. Extrai, transforma e limpa dados de carts, products e users.
    2. Carrega os dados tratados no PostgreSQL.
    3. Retorna os DataFrames resultantes para validação ou testes.
    """
    logging.info("Iniciando pipeline ETL completo")

    try:
        # ETL de carts (extração e transformação)
        logging.info("Executando ETL de carts...")
        data_carts = run_etl_carts()  # Chama função que retorna DataFrame limpo de carrinhos
        logging.info(f"ETL de carts finalizado com {len(data_carts)} registros válidos")

        # ETL de products (extração e transformação)
        logging.info("Executando ETL de products...")
        data_products = run_etl_products()  # Chama função que retorna DataFrame limpo de produtos
        logging.info(f"ETL de products finalizado com {len(data_products)} registros válidos")

        # ETL de users (extração e transformação)
        logging.info("Executando ETL de users...")
        data_users = run_etl_users()  # Chama função que retorna DataFrame limpo de usuários
        logging.info(f"ETL de users finalizado com {len(data_users)} registros válidos")

        # Carga dos dados transformados no PostgreSQL
        logging.info("Iniciando carga dos dados no PostgreSQL...")
        run_load(data_carts, data_products, data_users)  # Chama função de carga ETL
        logging.info("Carga concluída com sucesso")

        executar_views(connect_db())

        logging.info("Pipeline ETL completo finalizado")
        return data_carts, data_products, data_users  # Retorna DataFrames para validação/testes

    except Exception as e:
        logging.error(f"Pipeline ETL falhou: {e}")  # Log de erro em caso de falha
        raise  # Propaga exceção para tratamento externo ou debug

def fetch_view(view_name: str, cursor) -> pd.DataFrame:
    """
    Retorna os dados de uma view como DataFrame.
    """
    cursor.execute(f"SELECT * FROM {view_name}")
    colnames = [desc[0] for desc in cursor.description]  # nomes das colunas
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=colnames)

def executar_views(conn):
    cursor = conn.cursor()
    view_names = [
        "vw_revenue_by_location",
        "vw_top_selling_product",
        "vw_top_brand_selling_state",
        "vw_rating_sales",
        "vw_top_selling_months"
    ]
    for view in view_names:
        print(f"\n===== {view} =====")
        df = fetch_view(view, cursor)
        print(df)

# Permite execução direta do script
if __name__ == "__main__":
    main()
