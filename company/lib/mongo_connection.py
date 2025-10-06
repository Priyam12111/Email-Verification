from lib.u_date import formatted_time

database_name = 'e-finder'


def mg_total_records(collection_name, cond, db):
    collection = db[collection_name]
    return collection.count_documents(cond)


def mg_list(collection_name, cond, db, sort_field=None, sort_order=1):
    collection = db[collection_name]
    if sort_field:
        return list(collection.find(cond).sort(sort_field, sort_order))
    else:
        return list(collection.find(cond))


def mg_one(collection_name, cond, db):
    collection = db[collection_name]
    return collection.find_one(cond)


def mg_random_one(collection_name, cond, db):
    collection = db[collection_name]
    query = {"$and": [cond]}
    pipeline = [{"$match": query}, {"$sample": {"size": 1}}]
    result = collection.aggregate(pipeline)
    return next(result, None)


def mg_aggregate_one(collection_name, cond, db):
    collection = db[collection_name]
    result = collection.aggregate(cond)
    return next(result, None)


def mg_aggregate(collection_name, cond, db):
    collection = db[collection_name]
    return list(collection.aggregate(cond))


def mg_insert(collection_name, new_data, db):
    try:
        collection = db[collection_name]
        new_data['createdAt'] = str(formatted_time())
        result = collection.insert_one(new_data)
        return result.inserted_id
    except Exception as e:
        # Handle the exception
        print(f"An error occurred: {e}")


def mg_update(collection_name, cond, db, new_data, ss="$set"):
    collection = db[collection_name]
    result = collection.update_one(cond, {ss: new_data})

    # Print the number of documents updated
    # print("Matched", result.matched_count, "document(s) and modified", result.modified_count, "document(s).")
    return {'matched_count': result.matched_count, 'modified_count': result.modified_count}


def mg_delete_one(collection_name, cond, db):
    collection = db[collection_name]
    result = collection.delete_one(cond)
    return result.deleted_count

