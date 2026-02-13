from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("MONGO_USER")
password = os.getenv("MONGO_PASSWORD")
host = os.getenv("MONGO_HOST")
door = os.getenv("MONGO_PORT")
base = os.getenv("MONGO_DB")

uri = f"mongodb://{username}:{password}@{host}:{door}/{base}?authSource=admin"

client = MongoClient(uri)

db = client[base]

collection = db['carts']

data = collection.find()