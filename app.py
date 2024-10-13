from flask import Flask, request, jsonify
import pandas as pd
import os
import tempfile
import json
import duckdb


from dotenv import load_dotenv
from flask_cors import CORS
from utils import *

# Load environment variables from .env file
load_dotenv()

# Fetch API key and URL
ANYTHINGLLM_API_KEY = os.getenv('ANYTHINGLLM_API_KEY')
ANYTHINGLLM_API_URL = os.getenv('ANYTHINGLLM_API_URL')

TRANSLATE_SQL_WORKSPACE=os.getenv('TRANSLATE_SQL_WORKSPACE')
app = Flask(__name__)

CORS(app)

# Create a global variable for the DataFrame
temp_df = None

# Endpoint to upload CSV and convert to Pandas DataFrame
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    global temp_df
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    try:
        # Save the uploaded file to a temporary directory
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, file.filename)
        file.save(file_path)
        
        # Read the CSV file into a Pandas DataFrame
        temp_df = pd.read_csv(file_path)
        # Print schema (DataFrame columns and data types) and first 10 rows
        schema = temp_df.dtypes.to_dict()
        print(schema)
        temp_df.head(5)
        
        response = {
            "message": "CSV uploaded and converted to DataFrame"
        }
        
        return jsonify(response), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint to execute SQL query on the DataFrame
@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    global temp_df
    if temp_df is None:
        return jsonify({"error": "No DataFrame found. Upload a CSV first."}), 400
    
    data = request.get_json()
    sql_query = data.get("sql")
    
    if not sql_query:
        return jsonify({"error": "No SQL query provided"}), 400
    
    try:
        # Use pandasql to execute SQL query on the DataFrame
        result_df = duckdb.sql(sql_query).df()
        
        # Convert the result to JSON
        result_json = result_df.to_dict(orient="records")
        
        return jsonify(result_json), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#AnythingLL workspace setting: chat history = 10, temp = 0.2
@app.route("/translate_sql", methods=["POST"])
def translate_sql():
    
    request_data = request.get_json()
    if 'query' not in request_data:
        return jsonify({"status": "Error", "message": "No query provided"}), 400
    schema_str = ', '.join([f'{col}: {dtype}' for col, dtype in zip(temp_df.columns, temp_df.dtypes)])
    data = {
        "message": f"""with this schema: {schema_str}, translate the query below into sql.
        For the FROM statement, always do 'FROM temp_df'
        Ensure that the output is sql statement only, with no additional text.

        Query: {request_data['query']}""",
        "mode": "query"
    }

    headers = {
        'Authorization': f'Bearer {ANYTHINGLLM_API_KEY}',
        'Content-Type': 'application/json'
    }
    print(f"Posting to endpoint: {ANYTHINGLLM_API_URL}/{TRANSLATE_SQL_WORKSPACE}/chat")
    result = post_to_api(f"{ANYTHINGLLM_API_URL}/{TRANSLATE_SQL_WORKSPACE}/chat", data, headers)

    if result["status"] == 200:
        text_response = result["data"].get("textResponse", "")
        
        return jsonify({"status": 200, "sql": clean_sql(text_response)})

    return jsonify({"status": "Error", "message": result["message"]}), result.get("status_code", 500)


if __name__ == '__main__':
    app.run(debug=True)
