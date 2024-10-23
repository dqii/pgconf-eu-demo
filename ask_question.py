import os
import sys
import numpy as np
from typing import Literal
from pgvector.psycopg2 import register_vector
import psycopg2
from dotenv import load_dotenv
from contextlib import contextmanager
from pgconf_utils import generate_openai_embedding, generate_ubicloud_embedding, ask_openai, ask_ubicloud

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


ContextFormat = Literal["Code Summaries", "Raw Code"]

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


def query_files(provider, repo, vector, top_k=5):
    FETCH_FILES = f"""
        SELECT "name", "code", "folder", llm_{provider}
        FROM files 
        WHERE repo = %s
        ORDER BY vector_{provider} <-> %s
        LIMIT %s
    """
    if type(vector) == list:
        vector = np.array(vector)
    with get_cursor() as cur:
        cur.execute(FETCH_FILES, (repo, vector, top_k))
        files = cur.fetchall()
        return files

def query_files_bm25(provider, repo, question, top_k=5):
    FETCH_FILES_BM25 = f"""
        SELECT name, code, folder, llm_{provider}
        FROM search_bm25('files', 'id', ARRAY['code_stemmed'], %s, result_limit =>100)
        LEFT JOIN files ON id = doc_id
        WHERE repo = %s
        LIMIT %s;
    """
    with get_cursor() as cur:
        cur.execute(FETCH_FILES_BM25, (question, repo, top_k))
        files = cur.fetchall()
        return files



def query_folders(provider, repo, vector, top_k=5):
    FETCH_FOLDERS = f"""
        SELECT "name", llm_{provider}
        FROM folders 
        WHERE repo = %s
        ORDER BY vector_{provider} <-> %s
        LIMIT %s
    """
    if type(vector) == list:
        vector = np.array(vector)
    with get_cursor() as cur:
        cur.execute(FETCH_FOLDERS, (repo, vector, top_k))
        folders = cur.fetchall()
        return folders


def query_commits(provider, repo, vector, top_k=5):
    FETCH_COMMITS = f"""
        SELECT "repo", "id", llm_{provider}
        FROM commits 
        WHERE repo = %s
        ORDER BY vector_{provider} <-> %s
        LIMIT %s
    """
    if type(vector) == list:
        vector = np.array(vector)
    with get_cursor() as cur:
        cur.execute(FETCH_COMMITS, (repo, vector, top_k))
        commits = cur.fetchall()
        return commits


def get_prompt(provider: str, repo: str, question: str, context_types, context_format: ContextFormat | None) -> str:
    if provider not in ["openai", "ubicloud"]:
        raise ValueError("Invalid provider. Must be 'openai' or 'ubicloud'.")

    vector = generate_openai_embedding(
        question) if provider == "openai" else generate_ubicloud_embedding(question)

    context = []

    if "folders" in context_types:
        folders = query_folders(provider, repo, vector)
        for folder in folders:
            name, description = folder
            context.append(f"FOLDER: {name}\nDESCRIPTION: {description}")

    if "files" in context_types:
        files = query_files(provider, repo, vector)
        for file in files:
            name, code, folder_name, description = file


            if context_format == "Raw Code":
                prompt_desc = code
            else:
                prompt_desc = description

            context.append(
                f"FILE: {name}\nFOLDER: {folder_name}\nDESCRIPTION:\n{prompt_desc}")

        # files_bm25 = query_files_bm25(provider, repo, question)
        # for file in files_bm25:
        #     name, code, folder_name, description = file
        #     context.append(
        #         f"FILE_BM25: {name}\nFOLDER: {folder_name}\nDESCRIPTION:\n{code}")

    if "commits" in context_types:
        commits = query_commits(provider, repo, vector)
        for commit in commits:
            repo, commit_id, description = commit
            context.append(
                f"COMMIT: {commit_id}\nDESCRIPTION: {description}\n\n")

    context_count = len(context)
    if context_count == 0:
        return f"Answer the question about the {repo} repo: {question}"

    context_string = '\n\n'.join(
        map(lambda i: f"**CONTEXT {i + 1} / {context_count}**\n" +
            context[i], range(context_count))
    )
    prompt = '\n'.join([f"Answer the question about the {repo} repo using the provided context. Cite specific portions of the given context if they were relevant to answering the question.",
                        '-------------------------------',
                        '**QUESTION**: ' + question,
                        '-------------------------------',
                        context_string,
                        ])
    return prompt


def ask_question(provider: str, repo: str, question: str, context_types = None, context_format: ContextFormat | None = None, return_prompt=False) -> str | tuple[str, str]:
    if provider not in ["openai", "ubicloud"]:
        raise ValueError("Invalid provider. Must be 'openai' or 'ubicloud'.")

    user_prompt = get_prompt(provider, repo, question, context_types, context_format)
    system_prompt = f"You are a helpful agent who answers questions about the {repo} codebase. You will be given context about the codebase and asked questions about it. Please provide detailed answers to the best of your ability."
    ask = ask_openai if provider == "openai" else ask_ubicloud
    answer = ask(system_prompt, user_prompt)
    if return_prompt:
        return answer, user_prompt
    return answer


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python ask_question.py <provider> <repo> <question>")
        sys.exit(1)

    provider = sys.argv[1]
    repo_name = sys.argv[2]
    question = sys.argv[3]
    context_types = ["folders", "files", "commits"]

    prompt = get_prompt(provider, repo_name, question, context_types, "Code Summaries")
    print(prompt)

    answer = ask_question(provider, repo_name, question, context_types)
    print("Answer:")
    print(answer)
