import requests

def clean_sql(text):
    return text.replace("```sql\n", "").replace("```", "").strip()

def post_to_api(url, data, headers):
    response = requests.post(url, json=data, headers=headers)
    return {"status": response.status_code, "data": response.json()} if response.ok else {"status": "error", "message": response.text}

