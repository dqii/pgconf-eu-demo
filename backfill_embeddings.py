import os
import json
import argparse
from psycopg2.pool import ThreadedConnectionPool
from pgconf_utils import generate_openai_embedding, generate_ubicloud_embedding
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

MAX_WORKERS = 20
MAX_CONNECTIONS = 20
MIN_CONNECTIONS = 10

# DB connection
DATABASE_URL = os.getenv("DATABASE_URL")
connection_pool = ThreadedConnectionPool(
    MIN_CONNECTIONS, MAX_CONNECTIONS, DATABASE_URL)

# SQL queries to fetch repos, folders, and files missing embeddings
FETCH_FOLDERS = """SELECT "name", "llm_openai", "llm_ubicloud" FROM folders WHERE "vector_openai" IS NULL AND "repo" = %s"""
FETCH_FILES = """SELECT "name", "folder", "llm_openai", "llm_ubicloud" FROM files WHERE "vector_openai" IS NULL AND "repo" = %s"""
FETCH_COMMITS = """SELECT "repo", "id", "llm_openai", "llm_ubicloud" FROM commits WHERE "vector_openai" IS NULL AND "repo" = %s"""

# SQL queries to fetch repos, folders, and files missing embeddings -- override
FETCH_OVERRIDE_FOLDERS = """SELECT "name", "llm_openai", "llm_ubicloud" FROM folders WHERE "repo" = %s"""
FETCH_OVERRIDE_FILES = """SELECT "name", "folder", "llm_openai", "llm_ubicloud" FROM files WHERE "repo" = %s"""
FETCH_OVERRIDE_COMMITS = """SELECT "repo", "id", "llm_openai", "llm_ubicloud" FROM commits WHERE "repo" = %s"""

# SQL query to update embedding
UPDATE_EMBEDDING_FOLDER = """UPDATE folders SET vector_openai = %s, vector_ubicloud = %s, updated_at = now() WHERE "name" = %s AND repo = %s"""
UPDATE_EMBEDDING_FILE = """UPDATE files SET vector_openai = %s, vector_ubicloud = %s, updated_at = now() WHERE "name" = %s AND folder = %s AND repo = %s"""
UPDATE_EMBEDDING_COMMIT = """UPDATE commits SET vector_openai = %s, vector_ubicloud = %s, updated_at = now() WHERE "repo" = %s AND "id" = %s"""

# SQL query to update embedding -- OpenAI
UPDATE_EMBEDDING_FOLDER_OPENAI = """UPDATE folders SET vector_openai = %s, updated_at = now() WHERE "name" = %s AND repo = %s"""
UPDATE_EMBEDDING_FILE_OPENAI = """UPDATE files SET vector_openai = %s, updated_at = now() WHERE "name" = %s AND folder = %s AND repo = %s"""
UPDATE_EMBEDDING_COMMIT_OPENAI = """UPDATE commits SET vector_openai = %s, updated_at = now() WHERE "repo" = %s AND "id" = %s"""

# SQL query to update embedding -- Ubicloud
UPDATE_EMBEDDING_FOLDER_UBICLOUD = """UPDATE folders SET vector_ubicloud = %s, updated_at = now() WHERE "name" = %s AND repo = %s"""
UPDATE_EMBEDDING_FILE_UBICLOUD = """UPDATE files SET vector_ubicloud = %s, updated_at = now() WHERE "name" = %s AND folder = %s AND repo = %s"""
UPDATE_EMBEDDING_COMMIT_UBICLOUD = """UPDATE commits SET vector_ubicloud = %s, updated_at = now() WHERE "repo" = %s AND "id" = %s"""


def get_db_connection():
    return connection_pool.getconn()


def release_db_connection(conn):
    connection_pool.putconn(conn)


def update_folder_embedding(repo, folder, provider):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            name, llm_openai, llm_ubicloud = folder
            vector_openai = vector_ubicloud = None

            if llm_openai and (not provider or provider == 'openai'):
                vector_openai = generate_openai_embedding(llm_openai)
            if llm_ubicloud and (not provider or provider == 'ubicloud'):
                vector_ubicloud = generate_ubicloud_embedding(llm_ubicloud)

            if vector_openai and vector_ubicloud:
                cur.execute(UPDATE_EMBEDDING_FOLDER,
                            (json.dumps(vector_openai), json.dumps(vector_ubicloud), name, repo))
            elif vector_openai:
                cur.execute(UPDATE_EMBEDDING_FOLDER_OPENAI,
                            (json.dumps(vector_openai), name, repo))
            elif vector_ubicloud:
                cur.execute(UPDATE_EMBEDDING_FOLDER_UBICLOUD,
                            (json.dumps(vector_ubicloud), name, repo))
            conn.commit()
    finally:
        release_db_connection(conn)


def update_file_embedding(repo, file, provider):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            name, folder, llm_openai, llm_ubicloud = file
            vector_openai = vector_ubicloud = None

            if llm_openai and (not provider or provider == 'openai'):
                vector_openai = generate_openai_embedding(llm_openai)
            if llm_ubicloud and (not provider or provider == 'ubicloud'):
                vector_ubicloud = generate_ubicloud_embedding(llm_ubicloud)

            if vector_openai and vector_ubicloud:
                cur.execute(UPDATE_EMBEDDING_FILE,
                            (json.dumps(vector_openai), json.dumps(vector_ubicloud), name, folder, repo))
            elif vector_openai:
                cur.execute(UPDATE_EMBEDDING_FILE_OPENAI,
                            (json.dumps(vector_openai), name, folder, repo))
            elif vector_ubicloud:
                cur.execute(UPDATE_EMBEDDING_FILE_UBICLOUD,
                            (json.dumps(vector_ubicloud), name, folder, repo))
            conn.commit()
    finally:
        release_db_connection(conn)


def update_commit_embedding(repo, commit, provider):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            repo, commit_id, llm_openai, llm_ubicloud = commit
            vector_openai = vector_ubicloud = None

            if llm_openai and (not provider or provider == 'openai'):
                vector_openai = generate_openai_embedding(llm_openai)
            if llm_ubicloud and (not provider or provider == 'ubicloud'):
                vector_ubicloud = generate_ubicloud_embedding(llm_ubicloud)

            if vector_openai and vector_ubicloud:
                cur.execute(UPDATE_EMBEDDING_COMMIT,
                            (json.dumps(vector_openai), json.dumps(vector_ubicloud), repo, commit_id))
            elif vector_openai:
                cur.execute(UPDATE_EMBEDDING_COMMIT_OPENAI,
                            (json.dumps(vector_openai), repo, commit_id))
            elif vector_ubicloud:
                cur.execute(UPDATE_EMBEDDING_COMMIT_UBICLOUD,
                            (json.dumps(vector_ubicloud), repo, commit_id))
            conn.commit()
    finally:
        release_db_connection(conn)


def backfill_folders(repo, provider=None, override=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = FETCH_OVERRIDE_FOLDERS if override else FETCH_FOLDERS
            if override:
                query += " AND updated_at < %s"
                cur.execute(query, (repo, override))
            else:
                cur.execute(query, (repo,))
            folders = cur.fetchall()
        print(f"Backfilling {len(folders)} folders...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(
                update_folder_embedding, repo, folder, provider) for folder in folders]
            for future in as_completed(futures):
                future.result()
        print("Backfilling for folders complete.")
    finally:
        release_db_connection(conn)


def backfill_files(repo, provider=None, override=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = FETCH_OVERRIDE_FILES if override else FETCH_FILES
            if override:
                query += " AND updated_at < %s"
                cur.execute(query, (repo, override))
            else:
                cur.execute(query, (repo,))
            files = cur.fetchall()
        print(f"Backfilling {len(files)} files...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(
                update_file_embedding, repo, file, provider) for file in files]
            for future in as_completed(futures):
                future.result()
        print("Backfilling for files complete.")
    finally:
        release_db_connection(conn)


def backfill_commits(repo, provider=None, override=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = FETCH_OVERRIDE_COMMITS if override else FETCH_COMMITS
            if override:
                query += " AND updated_at < %s"
                cur.execute(query, (repo, override))
            else:
                cur.execute(query, (repo,))
            commits = cur.fetchall()
        print(f"Backfilling {len(commits)} commits...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(
                update_commit_embedding, repo, commit, provider) for commit in commits]
            for future in as_completed(futures):
                future.result()
        print("Backfilling for commits complete.")
    finally:
        release_db_connection(conn)


def backfill(repo, provider=None, override=None):
    backfill_folders(repo, provider, override)
    backfill_files(repo, provider, override)
    backfill_commits(repo, provider, override)
    print("Backfilling complete.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Backfill embeddings with optional parameters.")
    parser.add_argument("repo", nargs='?', default=None,
                        help="The name of the repository to backfill (optional).")
    parser.add_argument(
        "--provider", choices=['openai', 'ubicloud'], help="Specify the provider.")
    parser.add_argument(
        "--override", help="Enable override mode, which accepts a timestamp for updated_at")

    args = parser.parse_args()

    backfill(args.repo, args.provider, args.override)

    # Close the connection pool after all work is done
    connection_pool.closeall()
