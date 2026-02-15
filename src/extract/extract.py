import logging
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_mongo_client():
    try:
        username = os.getenv("MONGO_USER")
        password = os.getenv("MONGO_PASSWORD")
        host = os.getenv("MONGO_HOST")
        port = os.getenv("MONGO_PORT")
        database = os.getenv("MONGO_DB")

        uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"
        client = MongoClient(uri)
        logging.info("Conexão com MongoDB estabelecida")
        return client, database
    except Exception as e:
        logging.error(f"Falha ao conectar no MongoDB: {e}")
        raise

def extract_collection(collection_name: str) -> pd.DataFrame:
    logging.info(f"Iniciando extração da coleção '{collection_name}'")
    client, database = get_mongo_client()
    try:
        db = client[database]
        collection = db[collection_name]
        data = list(collection.find())
        if not data:
            logging.warning(f"Coleção '{collection_name}' está vazia")
        df = pd.DataFrame(data)
        logging.info(f"Extração da coleção '{collection_name}' concluída: {len(df)} registros")
    except Exception as e:
        logging.error(f"Erro ao extrair coleção '{collection_name}': {e}")
        raise
    finally:
        client.close()
        logging.info("Conexão com MongoDB fechada")
    
    return df
