import os
import sys
import numpy as np
from dotenv import load_dotenv
from pgconf_utils import generate_openai_embedding, generate_ubicloud_embedding, ask_openai, ask_ubicloud, get_cursor

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


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


def get_prompt(provider: str, repo: str, question: str, context_types) -> str:
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
            context.append(
                f"FILE: {name}\nFOLDER: {folder_name}\nDESCRIPTION:\n{description}")

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


def ask_question(provider: str, repo: str, question: str, context_types, return_prompt=False) -> str:
    if provider not in ["openai", "ubicloud"]:
        raise ValueError("Invalid provider. Must be 'openai' or 'ubicloud'.")

    prompt = get_prompt(provider, repo, question, context_types)
    ask = ask_openai if provider == "openai" else ask_ubicloud
    answer = ask(prompt)
    if return_prompt:
        return answer, prompt
    return answer


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python ask_question.py <provider> <repo> <question>")
        sys.exit(1)

    provider = sys.argv[1]
    repo_name = sys.argv[2]
    question = sys.argv[3]
    context_types = ["folders", "files", "commits"]

    prompt = get_prompt(provider, repo_name, question, context_types)
    print(prompt)

    answer = ask_question(provider, repo_name, question)
    print("Answer:")
    print(answer)
