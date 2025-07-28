
from pymongo import MongoClient
from configs.logger import log

# client = MongoClient('mongodb://developer:ah6M6vIz52YYJzy1@3.109.96.163:27017/e-finder?authSource=e-finder&readPreference=primary&serverSelectionTimeoutMS=20000&appname=mongosh%201.6.1&directConnection=true&ssl=false')
client = MongoClient('mongodb+srv://admin:jf0AY6Gw1vfnt5sn@cluster0.lgbepit.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
log.info("Connected to MongoDB")
db = client["e-finder"]
users = db["users-1"]
company = db["company-1"]
catch_all_patterns = db["catch_all_patterns"]