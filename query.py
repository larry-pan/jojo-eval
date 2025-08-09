import requests
import json
from dotenv import dotenv_values
import pandas as pd

env_vars = dotenv_values(".env")
REQUEST_URL = env_vars.get("REQUEST_URL")
BEARER_TOKEN = env_vars.get("BEARER_TOKEN")
CONNECTION_STRING = env_vars.get("CONNECTION_STRING")

HEADERS = {
    "Authorization": BEARER_TOKEN,
    "Neon-Connection-String": CONNECTION_STRING,
    "Content-Type": "application/json"
}

def execute_query(query, params=[]):
    """Execute SQL query on Neon PostgreSQL database"""

    payload = {
        "query": query, 
        "params": params
    }
    response = requests.post(REQUEST_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    return response.json()


def get_chat_tables():
    """Find all chat-related tables"""

    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_name LIKE '%chat%' OR table_name LIKE '%ai%';
    """
    return execute_query(query)

def get_ai_chat_structure():
    """Get structure of ai_chat table"""

    query = """
    SELECT column_name, data_type, is_nullable 
    FROM information_schema.columns 
    WHERE table_name = 'ai_chat' 
    ORDER BY ordinal_position;
    """
    return execute_query(query)

def get_chats_with_messages(limit=10):
    """Get ai_chats that have actual messages"""

    query = f"""
    SELECT * FROM ai_chat 
    WHERE json_array_length(messages::json) > 0 
    LIMIT {limit};
    """
    return execute_query(query)

def to_dataframe(results):
    """Convert query json results to DataFrame"""

    columns = [field['name'] for field in results['fields']]
    df = pd.DataFrame(results['rows'], columns=columns)
    return df

def output(data):
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# output(get_chats_with_messages())
# print(to_dataframe(get_chats_with_messages()))