from flask import Flask, jsonify, request, Response
import LAB_3
import json

app = Flask(__name__)


@app.route('/')
def home():
    return '''<h1>Welcome to the Yelp Data REST API</h1> 
              <p>Use the following endpoints:</p> 
              <ul> 
                <li><a href="/count_business_by_city_and_stars">Count Businesses by City and Stars</a> (GET)</li> 
                <li><a href="/find_reviews_for_high_rating_businesses">Find Reviews for High-Rating Businesses</a> (GET)</li> 
                <li><a href="/find_reviews_for_low_rating_businesses">Find Reviews for Low-Rating Businesses</a> (GET)</li> 
              </ul>'''


@app.route('/import_data', methods=['POST'])
def api_import_data():
    content = request.json
    collection_name = content['collection_name']
    file_path = content['file_path']
    result = LAB_3.import_json_lines_to_mongo(file_path, collection_name)
    return jsonify({"message": result})


@app.route('/count_business_by_city_and_stars', methods=['GET'])
def api_count_business_by_city_and_stars():
    result = LAB_3.count_business_by_city_and_stars()
    return jsonify(result)


# Streaming generator function to handle high-rating reviews
def stream_high_rating_reviews():
    yield '['  # Start of JSON array
    first = True
    for review in LAB_3.find_reviews_for_high_rating_businesses():
        if not first:
            yield ','  # Separate JSON objects with a comma
        first = False
        yield json.dumps(review)  # Convert each document to JSON
    yield ']'  # End of JSON array


@app.route('/find_reviews_for_high_rating_businesses', methods=['GET'])
def api_find_reviews_for_high_rating_businesses():
    return Response(stream_high_rating_reviews(), mimetype='application/json')


# Standard endpoint for low-rating reviews
@app.route('/find_reviews_for_low_rating_businesses', methods=['GET'])
def api_find_reviews_for_low_rating_businesses():
    result = LAB_3.find_reviews_for_low_rating_businesses()
    return jsonify({"message": result})


if __name__ == '__main__':
    app.run(debug=True)
