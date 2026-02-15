import logging
import pandas as pd
from .transform.transform_carts import run_etl_carts
from .transform.transform_products import run_etl_products
from .transform.transform_users import run_etl_users
from .load.load import run_load

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main() -> pd.DataFrame:
    logging.info("Iniciando pipeline ETL completo")

    try:
        # ETL de carts
        logging.info("Executando ETL de carts...")
        data_carts = run_etl_carts()
        logging.info(f"ETL de carts finalizado com {len(data_carts)} registros válidos")

        # ETL de products
        logging.info("Executando ETL de products...")
        data_products = run_etl_products()
        logging.info(f"ETL de products finalizado com {len(data_products)} registros válidos")

        # ETL de users
        logging.info("Executando ETL de users...")
        data_users = run_etl_users()
        logging.info(f"ETL de users finalizado com {len(data_users)} registros válidos")

        # Carga para PostgreSQL
        logging.info("Iniciando carga dos dados no PostgreSQL...")
        run_load(data_carts, data_products, data_users)
        logging.info("Carga concluída com sucesso")

        logging.info("Pipeline ETL completo finalizado")
        return data_carts, data_products, data_users

    except Exception as e:
        logging.error(f"Pipeline ETL falhou: {e}")
        raise

if __name__ == "__main__":
    main()
