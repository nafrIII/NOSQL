# Calculate the average price of each publishing company and sort by highest price
pipeline = [
    {"$group": {
        "_id": "$book.publisher", # Groups documents by publisher
        "avg_price": {
            "$avg": "$book.price" # Calculate the average price of the books from each publishing company
            }
        }
    },
    {"$sort":{"avg_price":-1}}, # Sort by avg_price in descending order
    {"$limit":5} # Limits to top 5 prices
]

result = orders.aggregate(pipeline)

# Prints results
for x in result:
    print(x)

# Find books that cost more than $5 and are shipped to countries starting with 'A'. Also return the book title in ascending order.
pipeline = [
    {"$match": {"book.price": {"$gt": 5}}},
    {"$match": {"shipping_address.country": {"$regex": "^A"}}},
    { "$group": {
        "_id": "$book.title",
        "Order_info": {
            "$push": {
                "price": "$book.price",
                "country_shipped_to": "$shipping_address.country"
            }
        }
    }},
    {"$sort": {"_id": 1}}
]

results = db.orders.aggregate(pipeline)
for res in results:
    print (res)

# Top 5 Countries by Publisher Count
pipeline = [
    {"$group": {"_id": "$shipping_address.country", "publishers": {"$addToSet": "$book.publisher"}}},
    {"$project": {"country": "$_id", "publishers": 1, "publisher_count": {"$size": "$publishers"}}},
    {"$unwind": "$publishers"},
    {"$group": {"_id": {"country": "$country", "publisher": "$publishers"}, "count": {"$sum": 1}}},
    {"$sort": {"_id.count": -1, "_id.country": 1}},  # Sort by count descending, then country
    {"$limit": 5},  # Limit to top 5 results
    {"$group": {"_id": "$_id.country", "publishers": {"$push": {"publisher": "$_id.publisher", "count": "$count"}}}},
]

results = orders.aggregate(pipeline)

for result in results:
    print(f"Country: {result['_id']}")
    for publisher in result['publishers']:
        print(f"  - Publisher: {publisher['publisher']}, Count: {publisher['count']}")
