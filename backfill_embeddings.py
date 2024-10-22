import os
import sys
import json
import psycopg2
from pgconf_utils import generate_openai_embedding, generate_ubicloud_embedding
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

MAX_WORKERS = 200

# DB connection
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# SQL queries to fetch repos, folders, and files missing embeddings
FETCH_FOLDERS = """SELECT "name", "llm_openai", "llm_ubicloud" FROM folders WHERE "vector_openai" IS NULL AND "repo" = %s;"""
FETCH_FILES = """SELECT "name", "folder", "llm_openai", "llm_ubicloud" FROM files WHERE "vector_openai" IS NULL AND "repo" = %s;"""
FETCH_COMMITS = """SELECT "repo", "id", "llm_openai", "llm_ubicloud" FROM commits WHERE "vector_openai" IS NULL AND "repo" = %s;"""

# SQL query to update embedding
UPDATE_EMBEDDING_FOLDER = """UPDATE folders SET vector_openai = %s, vector_ubicloud = %s WHERE "name" = %s AND repo = %s"""
UPDATE_EMBEDDING_FILE = """UPDATE files SET vector_openai = %s, vector_ubicloud = %s WHERE "name" = %s AND folder = %s AND repo = %s;"""
UPDATE_EMBEDDING_COMMIT = """UPDATE commits SET vector_openai = %s, vector_ubicloud = %s WHERE "repo" = %s AND "id" = %s;"""


def update_folder_embedding(repo, folder):
    name, llm_openai, llm_ubicloud = folder
    if llm_openai:
        vector_ubicloud = generate_ubicloud_embedding(llm_ubicloud)
        vector_openai = generate_openai_embedding(llm_openai)
        cur.execute(UPDATE_EMBEDDING_FOLDER,
                    (json.dumps(vector_openai), json.dumps(vector_ubicloud), name, repo))
        conn.commit()


def update_file_embedding(repo, file):
    name, folder, llm_openai, llm_ubicloud = file
    if llm_openai:
        vector_ubicloud = generate_ubicloud_embedding(llm_ubicloud)
        vector_openai = generate_openai_embedding(llm_openai)
        cur.execute(UPDATE_EMBEDDING_FILE,
                    (json.dumps(vector_openai), json.dumps(vector_ubicloud), name, folder, repo))
        conn.commit()


def update_commit_embedding(repo, commit):
    repo, commit_id, llm_openai, llm_ubicloud = commit
    if llm_openai:
        vector_ubicloud = generate_ubicloud_embedding(llm_ubicloud)
        vector_openai = generate_openai_embedding(llm_openai)
        cur.execute(UPDATE_EMBEDDING_COMMIT,
                    (json.dumps(vector_openai), json.dumps(vector_ubicloud), repo, commit_id))
        conn.commit()


def backfill_folders(repo):
    cur.execute(FETCH_FOLDERS, (repo, ))
    folders = cur.fetchall()
    print(f"Backfilling {len(folders)} folders...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(
            update_folder_embedding, repo, folder) for folder in folders]
        for future in as_completed(futures):
            future.result()  # Raise exceptions if any occurred during processing

    print("Backfilling for folders complete.")


def backfill_files(repo):
    cur.execute(FETCH_FILES, (repo, ))
    files = cur.fetchall()
    print(f"Backfilling {len(files)} files...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(
            update_file_embedding, repo, file) for file in files]
        for future in as_completed(futures):
            future.result()

    print("Backfilling for files complete.")


def backfill_commits(repo):
    cur.execute(FETCH_COMMITS, (repo, ))
    commits = cur.fetchall()
    print(f"Backfilling {len(commits)} commits...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(
            update_commit_embedding, repo, commit) for commit in commits]
        for future in as_completed(futures):
            future.result()

    print("Backfilling for commits complete.")


def backfill(repo):
    backfill_folders(repo)
    backfill_files(repo)
    backfill_commits(repo)
    print("Backfilling complete.")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python backfill_embeddings.py <repo_name>")
        sys.exit(1)
    repo = sys.argv[1]
    backfill(repo)

    cur.close()
    conn.close()
