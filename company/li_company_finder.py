from consumer import *
import pymongo

client = pymongo.MongoClient(
    "mongodb://admin:AdminStrongPass123@14.195.222.181:8989/admin"
)
db = client["e-finder"]["company-1"]
if __name__ == "__main__":
    db.find_one({"name_lc": re.compile("^flint international", re.IGNORECASE)})
    # with open("company\\formatter.csv", "r") as f:
    #     data = f.read()
    # sample_data = list(data.splitlines())

    # li_page = print(process(sample_data, 1))
