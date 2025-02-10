from pymongo import MongoClient, ASCENDING
import csv
import json

# Establish a connection to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')
db = client['yelp']

# Apply indexing to improve performance
db.business.create_index([("categories", ASCENDING), ("city", ASCENDING), ("stars", ASCENDING)])
db.business.create_index([("business_id", ASCENDING)])
db.review.create_index([("business_id", ASCENDING), ("stars", ASCENDING)])


def import_json_lines_to_mongo(file_path, collection_name):
    collection = db[collection_name]
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line:
                data = json.loads(line)
                collection.insert_one(data)
    print(f"Data imported to the '{collection_name}' collection successfully.")


def count_business_by_city_and_stars():
    pipeline_q1 = [
        {
            "$match": {
                "categories": {"$regex": ".*(Fast Food|Restaurants).*"}
            }
        },
        {
            "$group": {
                "_id": {"city": "$city", "stars": "$stars"},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "_id.city": 1,
                "_id.stars": 1
            }
        }
    ]
    result_q1 = list(db.business.aggregate(pipeline_q1))
    # Print the results
    for doc in result_q1:
        print(f"City: {doc['_id']['city']}, Stars: {doc['_id']['stars']}, Count: {doc['count']}")
    return [f"City: {doc['_id']['city']}, Stars: {doc['_id']['stars']}, Count: {doc['count']}" for doc in result_q1]


def write_results_to_csv(result, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["review_id", "business_id", "stars", "review_text"])
        writer.writeheader()
        writer.writerows({k: str(v) for k, v in doc.items()} for doc in result)


def find_reviews_for_high_rating_businesses():
    pipeline_q2_1 = [
        {
            "$match": {
                "categories": {"$regex": ".*(Fast Food|Restaurants).*"},
                "review_count": {"$gt": 10},
                "stars": {"$gte": 4}
            }
        },
        {
            "$lookup": {
                "from": "review",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "reviews"
            }
        },
        {"$unwind": "$reviews"},
        {"$match": {"reviews.stars": {"$gte": 4}}},
        {
            "$project": {
                "_id": 0,
                "review_id": "$reviews.review_id",
                "business_id": "$reviews.business_id",
                "stars": "$reviews.stars",
                "review_text": {"$ifNull": ["$reviews.text", ""]}
            }
        },
        {"$limit": 1000}
    ]
    result_q2_1 = list(db.business.aggregate(pipeline_q2_1))
    write_results_to_csv(result_q2_1, 'Q2_1_reviews.csv')
    print("Q2_1 results saved to 'Q2_1_reviews.csv'.")
    return result_q2_1


def find_reviews_for_low_rating_businesses():
    pipeline_q2_2 = [
        {
            "$match": {
                "categories": {"$regex": ".*(Fast Food|Restaurants).*"},
                "review_count": {"$gt": 10},
                "stars": {"$lt": 2}
            }
        },
        {
            "$lookup": {
                "from": "review",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "reviews"
            }
        },
        {"$unwind": "$reviews"},
        {"$match": {"reviews.stars": {"$lt": 2}}},
        {
            "$project": {
                "_id": 0,
                "review_id": "$reviews.review_id",
                "business_id": "$reviews.business_id",
                "stars": "$reviews.stars",
                "review_text": {"$ifNull": ["$reviews.text", ""]}
            }
        }
    ]
    result_q2_2 = list(db.business.aggregate(pipeline_q2_2))
    write_results_to_csv(result_q2_2, 'Q2_2_reviews.csv')
    print("Q2_2 results saved to 'q2_2_reviews.csv'.")
    return result_q2_2


if __name__ == '__main__':
    import_json_lines_to_mongo('yelp_academic_dataset_business.json', 'business')
    import_json_lines_to_mongo('yelp_academic_dataset_review.json', 'review')

    count_business_by_city_and_stars()
    find_reviews_for_high_rating_businesses()
    find_reviews_for_low_rating_businesses()
