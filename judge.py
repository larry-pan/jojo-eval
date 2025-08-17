import json
import openai
import time
from query import get_chats
from dotenv import dotenv_values

env_vars = dotenv_values(".env")
OPENAI_KEY = env_vars.get("OPENAI_KEY")
MODEL = "gpt-5-nano"
ALL_FEEDBACK_FILE = "detailed_judge_feedback.json"
CONSOLIDATED_FEEDBACK_FILE = "judge_feedback.json"
client = openai.OpenAI(
    api_key=OPENAI_KEY,
)


def judge_one(conversation_text, chat_id="unknown"):
    """Get AI feedback on a single conversation"""

    prompt = f"""You will be given a JSON array containing the conversation between an International Bacchelaruette student and custom AI assistant.
    Take the folowing factors into account, considering how they influence one another, to identify problems with the AI.
    Give examples from the conversation as relevant.
    
    Focus on:
    1. Conversation length
    2. Why the conversation ended (out of satisfaction, frustration, etc.)
    3. Sentiment analysis of the user's prompts
    4. Relevancy and efficacy of toolInvocations calls, if that field key name is used in the messages array

    Conversation:
    {conversation_text}
    
    Please provide structured critque with specific examples. Keep it concise and direct."""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": "You will be given a JSON array containing the conversation between an International Bacchelaruette student and custom AI assistant.",
            },
            {"role": "user", "content": prompt},
        ],
    )

    feedback = response.choices[0].message.content

    return {
        "chat_id": chat_id,
        "feedback": feedback,
        "judge_model": MODEL,
    }


def judge_all(limit, include_empty_chats):
    """Get ai feedback on all conversations"""

    print(f"Fetching {limit} conversations from database...")

    chats_df = get_chats(limit=limit, include_empty_chats=include_empty_chats)
    num_retrieved = len(chats_df)
    print(f"Retrieved {len(chats_df)} conversations")

    all_feedback = []
    all_feedback_with_messages = []

    for index, chat_row in chats_df.iterrows():
        chat_id = chat_row.get("id", f"chat_{index}")
        print(f"Analyzing conversation {index + 1}/{num_retrieved} (ID: {chat_id})")

        conversation_text = str(chat_row.get("messages"))

        feedback = judge_one(conversation_text, chat_id)
        all_feedback.append(feedback)

        feedback["original_messages"] = chat_row.get("messages")
        all_feedback_with_messages.append(feedback)

        time.sleep(0.5)  # avoid openai rate limit

    return all_feedback, all_feedback_with_messages


def consolidate_feedback(all_feedback):
    """Summarize feedback on all messages and identify key points"""

    prompt = f"""You will be given a JSON array containing the problems identified in converstions
    between an International Bacchelaruette student and custom AI assistant.
    
    You will:
    1. Identify the key and recurring problems mentioned
    2. Suggest actionable ideas to imrpove the AI assistant
    3. Include examples as relevant

    The critique:
    {all_feedback}
    
    Keep your response very short and concise"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": "You will pin down problems in chats between an International Bacchelaruette student and custom AI assistant.",
            },
            {"role": "user", "content": prompt},
        ],
    )

    feedback = response.choices[0].message.content

    return {
        "feedback": feedback,
        "judge_model": MODEL,
    }


def save_json(feedback_results_with_messages, consolidated_feedback):
    """Save feedback to json"""

    with open(ALL_FEEDBACK_FILE, "w", encoding="utf-8") as f:
        json.dump(
            feedback_results_with_messages, f, indent=2, ensure_ascii=False, default=str
        )
    with open(CONSOLIDATED_FEEDBACK_FILE, "w", encoding="utf-8") as f:
        json.dump(consolidated_feedback, f, indent=2, ensure_ascii=False, default=str)

    print(
        f"All detailed feedback saved to {ALL_FEEDBACK_FILE}\nConsolidated feedback saved to {CONSOLIDATED_FEEDBACK_FILE}"
    )


def judge(limit=2, include_empty_chats=False):
    """Judge chats"""
    print(f"Judging with {MODEL}")
    print(f"- Conversations to analyze: {limit}")
    print(f"- Include empty chats: {include_empty_chats}")

    feedback_results, feedback_results_with_messages = judge_all(
        limit=limit, include_empty_chats=include_empty_chats
    )
    consolidated_feedback = consolidate_feedback(feedback_results)

    save_json(feedback_results_with_messages, consolidated_feedback)
    print(f"Judging complete")


# judge(limit=5)
