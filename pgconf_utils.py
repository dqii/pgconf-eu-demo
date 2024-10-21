import os
import requests
from openai import OpenAI
from dotenv import load_dotenv
import psycopg2
from pgvector.psycopg2 import register_vector
from contextlib import contextmanager
load_dotenv()

# Database
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


# Open AI
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_KEY)
OPENAI_LLM_MODEL = "gpt-4o-mini"
OPENAI_VECTOR_MODEL = "text-embedding-3-small"
OPENAI_CONTEXT_WINDOW = 128000

# Ubicloud
UBICLOUD_LLM_API_URL = 'https://llama-3-2-3b-it.ai.ubicloud.com/v1/chat/completions'
UBICLOUD_VECTOR_API_URL = 'https://e5-mistral-7b-it.ai.ubicloud.com/v1/embeddings'
UBICLOUD_API_KEY = os.getenv("UBICLOUD_API_KEY")
UBICLOUD_CONTEXT_WINDOW = 90000
UBICLOUD_LLM_MODEL = "llama-3-2-3b-it"
UBICLOUD_VECTOR_MODEL = "e5-mistral-7b-it"

# Context window
CONTEXT_WINDOW = min(OPENAI_CONTEXT_WINDOW, UBICLOUD_CONTEXT_WINDOW)


@contextmanager
def get_cursor():
    conn = psycopg2.connect(DATABASE_URL)
    register_vector(conn)
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def generate_openai_embedding(text: str) -> list:
    response = client.embeddings.create(model=OPENAI_VECTOR_MODEL, input=text)
    embedding = response.data[0].embedding
    return embedding


def generate_ubicloud_embedding(text: str) -> list:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {UBICLOUD_API_KEY}"
    }

    data = {
        "model": UBICLOUD_VECTOR_MODEL,
        "input": text
    }

    response = requests.post(UBICLOUD_VECTOR_API_URL,
                             headers=headers, json=data)

    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")

    response = response.json()
    return response['data'][0]['embedding']


def ask_openai(prompt: str) -> str:
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model=OPENAI_LLM_MODEL,
    )
    response = chat_completion.choices[0].message.content
    if not response:
        raise Exception("No response from OpenAI")
    return response.strip()


def ask_ubicloud(prompt: str) -> str:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {UBICLOUD_API_KEY}"
    }
    data = {
        "model": UBICLOUD_LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }
    response = requests.post(UBICLOUD_LLM_API_URL, headers=headers, json=data)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    response_data = response.json()
    return response_data["choices"][0]["message"]["content"].strip()
