from .transform.transform_carts import run_etl_carts
from .transform.transform_products import run_etl_products
from .transform.transform_users import run_etl_users
from .load.load import run_load
import pandas as pd



def main() -> pd.DataFrame:
    data_carts = run_etl_carts()
    data_products = run_etl_products()
    data_users = run_etl_users()
    run_load(data_carts, data_products, data_users)
    return data_users # corrigir return

if __name__ == "__main__":
    main()
