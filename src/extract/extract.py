import logging
from pymongo import MongoClient        # Biblioteca para conexão e operações no MongoDB
from dotenv import load_dotenv        # Para carregar variáveis de ambiente de um arquivo .env
import os                             # Para acessar variáveis de ambiente
import pandas as pd                   # Para manipulação de dados em DataFrames

# Carrega as variáveis do arquivo .env para uso no código
load_dotenv()

# Configuração de logging para acompanhar execução e erros
logging.basicConfig(
    level=logging.INFO,               # Nível de log INFO (inclui INFO, WARNING e ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato de log com timestamp, nível e mensagem
)

# Função para criar e retornar o cliente do MongoDB
def get_mongo_client():
    try:
        # Recupera credenciais e informações de conexão do MongoDB do ambiente
        username = os.getenv("MONGO_USER")
        password = os.getenv("MONGO_PASSWORD")
        host = os.getenv("MONGO_HOST")
        port = os.getenv("MONGO_PORT")
        database = os.getenv("MONGO_DB")

        # Cria URI de conexão com autenticação
        uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"
        client = MongoClient(uri)
        logging.info("Conexão com MongoDB estabelecida")  # Log de sucesso
        return client, database
    except Exception as e:
        logging.error(f"Falha ao conectar no MongoDB: {e}")  # Log de erro em caso de falha
        raise  # Propaga o erro para o chamador tratar

# Função para extrair uma coleção do MongoDB como DataFrame do pandas
def extract_collection(collection_name: str) -> pd.DataFrame:
    logging.info(f"Iniciando extração da coleção '{collection_name}'")  # Log inicial
    client, database = get_mongo_client()  # Obtém cliente e nome do banco
    try:
        db = client[database]                  # Seleciona o banco de dados
        collection = db[collection_name]       # Seleciona a coleção desejada
        data = list(collection.find())         # Recupera todos os documentos da coleção como lista de dicionários
        if not data:
            logging.warning(f"Coleção '{collection_name}' está vazia")  # Log caso não haja registros
        df = pd.DataFrame(data)                # Converte lista de documentos em DataFrame do pandas
        logging.info(f"Extração da coleção '{collection_name}' concluída: {len(df)} registros")  # Log final
    except Exception as e:
        logging.error(f"Erro ao extrair coleção '{collection_name}': {e}")  # Log de erro caso algo falhe
        raise
    finally:
        client.close()                         # Fecha conexão com MongoDB para liberar recursos
        logging.info("Conexão com MongoDB fechada")
    
    return df  # Retorna o DataFrame com os dados da coleção
