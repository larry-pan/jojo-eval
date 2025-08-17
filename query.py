import json
from dotenv import dotenv_values
from sqlalchemy import create_engine, text
import pandas as pd

env_vars = dotenv_values(".env")
CONNECTION_STRING = env_vars.get("CONNECTION_STRING")

engine = create_engine(CONNECTION_STRING)


def execute_query(query):
    """Execute sqlalchemy query on neon postgresql db and return df"""
    df = pd.read_sql_query(text(query), engine)
    return df


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


def get_chats(limit=10, include_empty_chats=True):
    """Get random sample of ai_chats that have actual messages"""

    estimate_query = """
    SELECT reltuples::bigint 
    FROM pg_class 
    WHERE relname = 'ai_chat';
    """
    result_df = execute_query(estimate_query)
    estimated_rows = result_df.iloc[0, 0] if not result_df.empty else 1000  # fallback

    # calculate percentage to get ~3x our target num of chats
    target_sample = limit * 3
    sample_percent = min(100, max(0.1, (target_sample / estimated_rows) * 100))

    query = f"""
    SELECT *
    FROM ai_chat TABLESAMPLE BERNOULLI({sample_percent})
    {"" if include_empty_chats else "WHERE messages IS NOT NULL AND jsonb_array_length(messages) > 0"}
    LIMIT {limit};
    """
    return execute_query(query)


def to_dataframe(results):
    """Convert json to df"""

    columns = [field["name"] for field in results["fields"]]
    df = pd.DataFrame(results["rows"], columns=columns)
    return df


def to_json(df):
    """Convert df to json"""

    records = df.to_dict("records")
    return json.dumps(records, indent=2, ensure_ascii=False, default=str)


def save_json(df, filename="output.json"):
    """Save df as json file"""
    records = df.to_dict("records")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False, default=str)
    print(f"Data saved to {filename}")
