from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

def get_mongo_client():
    username = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    database = os.getenv("MONGO_DB")

    uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"
    return MongoClient(uri), database


def extract_collection(collection_name: str) -> pd.DataFrame:
    client, database = get_mongo_client()
    db = client[database]
    collection = db[collection_name]
    data = list(collection.find())
    df = pd.DataFrame(data)

    return df
