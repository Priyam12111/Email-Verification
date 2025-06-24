# from pymongo import MongoClient
# from dotenv import load_dotenv
# import os

# load_dotenv()

# # client = MongoClient(os.environ['MONGO_URI'])
# # client = MongoClient(uri, serverSelectionTimeoutMS=20000)

# try:
#     # Connect to MongoDB
#     client = MongoClient(os.environ['MONGO_URI'], serverSelectionTimeoutMS=60000)
#     client = MongoClient('mongodb://developer:ah6M6vIz52YYJzy1@3.109.96.163:27017/e-finder?authSource=e-finder&readPreference=primary&serverSelectionTimeoutMS=20000&appname=mongosh%201.6.1', serverSelectionTimeoutMS=60000)
    
#     # Check if the connection is successful
#     print("Connected successfully!")
    
#     # database_name = os.environ['DB_NAME']
#     database_name = 'e-finder'
#     db = client[database_name]
#     users_collection = db['users']
#     url_collection = db['urls']
#     company_collection = db['company']
#     cred_collection = db['gmail-credential']
    
# except Exception as e:
#     print("Error:", e)
# # try:
# #     # The ismaster command is cheap and does not require auth.
# #     client.admin.command('ismaster')
# #     print("MongoDB connection successful")
# # except Exception as e:
# #     print(f"MongoDB connection error: {e}")


# # database_name = os.environ['DB_NAME']
# # db = client[database_name]
# # users_collection = db['users']
# # url_collection = db['urls']
# # company_collection = db['company']
# # cred_collection = db['gmail-credential']
