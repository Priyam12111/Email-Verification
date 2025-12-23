
from pymongo import MongoClient
from configs.logger import log

# client = MongoClient('mongodb://developer:ah6M6vIz52YYJzy1@3.109.96.163:27017/e-finder?authSource=e-finder&readPreference=primary&serverSelectionTimeoutMS=20000&appname=mongosh%201.6.1&directConnection=true&ssl=false')
# client = MongoClient('mongodb://admin:AdminStrongPass123@14.195.222.181:8989/e-finder?authSource=e-finder&readPreference=primary&serverSelectionTimeoutMS=20000&appname=mongosh%201.6.1&directConnection=true&ssl=false')

uri = (
    "mongodb://admin:AdminStrongPass123@14.195.222.181:8989/e-finder"
    "?authSource=admin"
    "&serverSelectionTimeoutMS=5000"
    "&connectTimeoutMS=5000"
    "&socketTimeoutMS=5000"
    "&directConnection=true"
)

client = MongoClient(uri)

log.info("Connected to MongoDB")
db = client["e-finder"]
users = db["users-new"]
company = db["company-1"]
catch_all_patterns = db["catch_all_patterns"]
pattern_stats = db["pattern_stats"]