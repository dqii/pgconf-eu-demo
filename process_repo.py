import os
import re
import sys
import argparse
from psycopg2.pool import ThreadedConnectionPool
from concurrent.futures import ThreadPoolExecutor, as_completed
from pgconf_utils import ask_openai, ask_ubicloud, OPENAI_CONTEXT_WINDOW, UBICLOUD_CONTEXT_WINDOW, CONTEXT_WINDOW
from dotenv import load_dotenv
from backfill_embeddings import backfill
from contextlib import contextmanager
load_dotenv()

MAX_WORKERS = 20
MIN_CONNECTIONS = 1
MAX_CONNECTIONS = 40

# Database connection pool
DATABASE_URL = os.getenv("DATABASE_URL")
connection_pool = ThreadedConnectionPool(
    MIN_CONNECTIONS, MAX_CONNECTIONS, DATABASE_URL)

# Prompts
FILE_PROMPT = """You are a helpful code assistant. You will receive code from a file, and you will summarize what that the code does, including specific interfaces where helpful."""
FILE_SUMMARIES_PROMPT = """You are a helpful code assistant. You will receive summaries of multiple sections of a file. You will summarize what the overall file does, given the sections, including specific interfaces where helpful."""
FOLDER_PROMPT = """You are a helpful code assistant. You will receive summaries of the files and subfolders in this folder. You will summarize what the folder does."""
FOLDER_SUMMARIES_PROMPT = """You are a helpful code assistant. You will receive summaries of the files and subfolders in this folder. You will summarize what the folder does."""
COMMIT_PROMPT = """You are a helpful code assistant. You will receive a commit, including the commit message, and the changes made in the commit. You will summarize the commit."""

# Queries
INSERT_COMMIT = """
    INSERT INTO commits ("repo", "id", "author", "date", "changes", "title", "message", "llm_openai", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("repo", "id") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai", "llm_ubicloud" = EXCLUDED."llm_ubicloud", "updated_at" = now();
"""
INSERT_COMMIT_OPENAI = """
    INSERT INTO commits ("repo", "id", "author", "date", "changes", "title", "message", "llm_openai")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("repo", "id") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai", "updated_at" = now();
"""
INSERT_COMMIT_UBICLOUD = """
    INSERT INTO commits ("repo", "id", "author", "date", "changes", "title", "message", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("repo", "id") DO UPDATE SET "llm_ubicloud" = EXCLUDED."llm_ubicloud", "updated_at" = now();
"""
INSERT_FOLDER = """
    INSERT INTO folders ("name", "repo", "llm_openai", "llm_ubicloud")
    VALUES (%s, %s, %s, %s)
    ON CONFLICT ("name", "repo") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai", "llm_ubicloud" = EXCLUDED."llm_ubicloud", "updated_at" = now();
"""
INSERT_FOLDER_OPENAI = """
    INSERT INTO folders ("name", "repo", "llm_openai")
    VALUES (%s, %s, %s)
    ON CONFLICT ("name", "repo") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai", "updated_at" = now();
"""
INSERT_FOLDER_UBICLOUD = """
    INSERT INTO folders ("name", "repo", "llm_ubicloud")
    VALUES (%s, %s, %s)
    ON CONFLICT ("name", "repo") DO UPDATE SET "llm_ubicloud" = EXCLUDED."llm_ubicloud", "updated_at" = now();
"""
INSERT_FILE = """
    INSERT INTO files ("name", "folder", "repo", "code", "llm_openai", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT ("name", "folder", "repo") DO UPDATE SET "code" = EXCLUDED."code", "llm_openai" = EXCLUDED."llm_openai", "llm_ubicloud" = EXCLUDED."llm_ubicloud", "updated_at" = now();
"""
INSERT_FILE_OPENAI = """
    INSERT INTO files ("name", "folder", "repo", "code", "llm_openai")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("name", "folder", "repo") DO UPDATE SET "code" = EXCLUDED."code", "llm_openai" = EXCLUDED."llm_openai", "updated_at" = now();
"""
INSERT_FILE_UBICLOUD = """
    INSERT INTO files ("name", "folder", "repo", "code", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("name", "folder", "repo") DO UPDATE SET "code" = EXCLUDED."code", "llm_ubicloud" = EXCLUDED."llm_ubicloud", "updated_at" = now();
"""


def get_db_connection():
    return connection_pool.getconn()


def release_db_connection(conn):
    connection_pool.putconn(conn)


# note: can be used in place of the two functions above as a context manager
# the function is currently unused, since I did not want to refactr a lot of the code,
# in case there is unpushed changes elsewhere
# usage example:
# with pool_connection() as conn:
#     with conn.cursor() as cur:
#         cur.execute("SELECT * FROM table")
# - connection is returned to the pool after the block is executed
# ...
@contextmanager
def pool_connection():
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)



def is_acceptable_file(file_name):
    ACCEPTABLE_SUFFIXES = [
        '.py', '.js', '.java', '.rb', '.go', '.rs', '.json', '.yaml', '.yml', '.xml',
        '.md', '.txt', '.sh', '.sql', '.ts', '.h', '.c', '.cpp', '.hpp', '.php',
        '.jsx', '.tsx', '.swift', '.kt', '.cs', '.out'
    ]
    ACCEPTABLE_FILENAMES = {'Makefile', 'Dockerfile', '.env'}
    REJECTED_FILENAMES = {'package-lock.json'}

    if file_name in REJECTED_FILENAMES:
        return False

    return any(file_name.endswith(suffix) for suffix in ACCEPTABLE_SUFFIXES) or file_name in ACCEPTABLE_FILENAMES


def is_acceptable_folder(folder_name):
    EXCLUDED_DIRS = {'.git', '.devcontainer', '.venv', 'node_modules'}
    path_parts = folder_name.split(os.sep)
    return not any(part in EXCLUDED_DIRS for part in path_parts)


def insert_repo(repo_name):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            INSERT_REPO = """INSERT INTO repos ("name") VALUES (%s) ON CONFLICT DO NOTHING;"""
            cur.execute(INSERT_REPO, (repo_name,))
        conn.commit()
    finally:
        release_db_connection(conn)


def insert_folder(folder_name, repo_name, llm_openai, llm_ubicloud):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if llm_openai and llm_ubicloud:
                cur.execute(INSERT_FOLDER, (folder_name, repo_name,
                            llm_openai.strip(), llm_ubicloud.strip()))
            elif llm_openai:
                cur.execute(INSERT_FOLDER_OPENAI,
                            (folder_name, repo_name, llm_openai.strip()))
            elif llm_ubicloud:
                cur.execute(INSERT_FOLDER_UBICLOUD,
                            (folder_name, repo_name, llm_ubicloud.strip()))
        conn.commit()
    finally:
        release_db_connection(conn)


def insert_file(file_name, folder_name, repo_name, file_content, llm_openai, llm_ubicloud):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if llm_openai and llm_ubicloud:
                cur.execute(INSERT_FILE, (file_name, folder_name, repo_name,
                            file_content, llm_openai.strip(), llm_ubicloud.strip()))
            elif llm_openai:
                cur.execute(INSERT_FILE_OPENAI, (file_name, folder_name,
                            repo_name, file_content, llm_openai.strip()))
            elif llm_ubicloud:
                cur.execute(INSERT_FILE_UBICLOUD, (file_name, folder_name,
                            repo_name, file_content, llm_ubicloud.strip()))
        conn.commit()
    finally:
        release_db_connection(conn)


def insert_commit(repo_name, commit_id, author, date, changes, title, message, llm_openai, llm_ubicloud):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if llm_openai and llm_ubicloud:
                cur.execute(INSERT_COMMIT, (repo_name, commit_id, author, date,
                            changes, title, message, llm_openai.strip(), llm_ubicloud.strip()))
            elif llm_openai:
                cur.execute(INSERT_COMMIT_OPENAI, (repo_name, commit_id,
                            author, date, changes, title, message, llm_openai.strip()))
            elif llm_ubicloud:
                cur.execute(INSERT_COMMIT_UBICLOUD, (repo_name, commit_id,
                            author, date, changes, title, message, llm_ubicloud.strip()))
        conn.commit()
    finally:
        release_db_connection(conn)


def chunk_file(file_content, context_window):
    """
    Splits the file content into chunks, ensuring that each chunk ends at a function boundary.
    Specifically, it looks for `}` at the beginning of a line as a natural break point.
    """
    chunks = []
    current_chunk = []
    current_size = 0

    # Split the content into lines for easier processing
    lines = file_content.splitlines()

    for line in lines:
        if len(line) > context_window:
            line = line[:(context_window)]
        current_chunk.append(line)
        current_size += len(line)

        # If we've reached a size limit
        if (
            (current_size >= context_window and re.match(
                r'^(\}|\};|\]|\];|\)|\);)$', line))
            or (current_size >= 2 * context_window and re.match(r'^\s{2}(\}|\};|\]|\];|\)|\);)$', line))
            or (current_size >= 3 * context_window)
        ):
            chunks.append("\n".join(current_chunk))
            current_chunk = []
            current_size = 0

    # Add the remaining chunk if any content is left
    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return chunks


def process_file(file_path, folder_name, repo_name, provider, override):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            file_name = os.path.basename(file_path)

            if not override:
                cur.execute(
                    """SELECT "llm_openai", "llm_ubicloud" FROM files WHERE "name" = %s AND "folder" = %s AND "repo" = %s""",
                    (file_name, folder_name, repo_name)
                )
            else:
                cur.execute(
                    """SELECT "llm_openai", "llm_ubicloud" FROM files WHERE "name" = %s AND "folder" = %s AND "repo" = %s AND updated_at > %s""",
                    (file_name, folder_name, repo_name, override)
                )
            row = cur.fetchone()
            if row:
                return row

            def get_description(chunks, ask):
                if len(chunks) == 1:
                    return ask(FILE_PROMPT, "File: " + file_name + "\n\n" + chunks[0])
                else:
                    descriptions = []
                    for chunk in chunks:
                        descriptions.append(
                            ask(FILE_PROMPT, "File: " + file_name + "\n\n" + chunk))
                    return ask(FILE_SUMMARIES_PROMPT, "File: " + file_name + "\n\n" + "\n".join(descriptions[:10]))

            print("File:", file_path)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                file_content = f.read()

            llm_openai, llm_ubicloud = None, None

            if provider is None or provider == 'openai':
                chunks_openai = chunk_file(file_content, OPENAI_CONTEXT_WINDOW)
                llm_openai = get_description(chunks_openai, ask_openai)
            if provider is None or provider == 'ubicloud':
                chunks_ubicloud = chunk_file(
                    file_content, UBICLOUD_CONTEXT_WINDOW)
                llm_ubicloud = get_description(chunks_ubicloud, ask_ubicloud)

            # Insert the file and its components into the database
            insert_file(file_name, folder_name, repo_name,
                        file_content, llm_openai, llm_ubicloud)

            return llm_openai, llm_ubicloud
    finally:
        release_db_connection(conn)


def process_files_in_folder(folder_path, repo_path, repo_name, provider, override):
    file_futures = []
    folder_name = os.path.relpath(folder_path, repo_path)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path) and is_acceptable_file(item):
                file_futures.append(executor.submit(
                    process_file, item_path, folder_name, repo_name, provider, override
                ))

        summaries = []
        for future in as_completed(file_futures):
            try:
                llm_openai, llm_ubicloud = future.result()
                summaries.append((llm_openai, llm_ubicloud))
            except Exception as e:
                print(f"Error processing file in folder '{folder_name}': {e}")

    return summaries


def get_description(descriptions, ask, context_window):
    max_descriptions = int(context_window / 480)
    if len(descriptions) < max_descriptions:
        return ask(FOLDER_PROMPT, "\n".join(descriptions))
    else:
        combined_descriptions = []
        for i in range(0, min(len(descriptions),  max_descriptions * max_descriptions), max_descriptions):
            combined_description = ask(
                FOLDER_PROMPT, "\n".join(descriptions[i:i+max_descriptions]))
            combined_descriptions.append(combined_description)
        return ask(FOLDER_SUMMARIES_PROMPT, "\n".join(combined_descriptions))


def process_folder(folder_path, repo_path, repo_name, provider, override):
    if not is_acceptable_folder(folder_path):
        return

    folder_name = os.path.relpath(folder_path, repo_path)
    print(f"Processing folder: {folder_name}")

    # If folder already has a summary, skip processing and just return it
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if not override:
                cur.execute(
                    """SELECT 1 FROM folders WHERE "name" = %s AND "repo" = %s""", (folder_name, repo_name))
            else:
                cur.execute(
                    """SELECT 1 FROM folders WHERE "name" = %s AND "repo" = %s AND updated_at > %s""", (folder_name, repo_name, override))
            row = cur.fetchone()
            if row:
                return

            # Fetch summaries for subfolders from the database
            cur.execute(
                """SELECT "llm_openai", "llm_ubicloud" FROM folders WHERE "repo" = %s AND "name" LIKE %s""",
                (repo_name, folder_name + '/%')
            )
            subfolder_summaries = cur.fetchall()

        # Process all files in the current folder
        file_summaries = process_files_in_folder(
            folder_path, repo_path, repo_name, provider, override)

        # Combine file summaries and subfolder summaries
        combined_summaries = file_summaries + [
            (llm_openai, llm_ubicloud) for llm_openai, llm_ubicloud in subfolder_summaries
        ]

        # Generate a folder summary from combined summaries
        if len(combined_summaries) > 0:
            llm_openai = llm_ubicloud = None
            if provider == None or provider == 'openai':
                llm_openai = get_description(
                    [summary[0] for summary in combined_summaries if summary[0]
                    ], ask_openai, OPENAI_CONTEXT_WINDOW
                )
            if provider == None or provider == 'ubicloud':
                llm_ubicloud = get_description(
                    [summary[1] for summary in combined_summaries if summary[1]
                    ], ask_ubicloud, UBICLOUD_CONTEXT_WINDOW
                )
            insert_folder(folder_name, repo_name, llm_openai, llm_ubicloud)
    finally:
        release_db_connection(conn)


def extract_files_changed(diff_content):
    """
    Extracts a list of files changed from the diff content.
    """
    files_changed = set()
    for line in diff_content.splitlines():
        if line.startswith('diff --git'):
            # Example line: diff --git a/file1.txt b/file1.txt
            parts = line.split()
            if len(parts) >= 3:
                # Extract the file path (removing the 'a/' or 'b/' prefix)
                file_path = parts[2].replace('b/', '').replace('a/', '')
                files_changed.add(file_path)
    return list(files_changed)


def process_commit(repo_name, commit_id, author, date, changes, title, message, provider):
    author_email = f"{author}"
    if len(changes) < CONTEXT_WINDOW:
        input_text = f"Title: {title}\nMessage: {message}\nChanges: {changes}\nAuthor: {author_email}\nDate: {date}"
    else:
        files_changed = extract_files_changed(changes)
        input_text = f"Title: {title}\nMessage: {message}\nFiles changed: {', '.join(files_changed)}\nAuthor: {author_email}\nDate: {date}"

    llm_ubicloud = llm_openai = None

    if provider is None or provider == 'openai':
        llm_openai = ask_openai(COMMIT_PROMPT, input_text)
    if provider is None or provider == 'ubicloud':
        llm_ubicloud = ask_ubicloud(COMMIT_PROMPT, input_text)

    insert_commit(repo_name, commit_id, author, date, changes,
                  title, message, llm_openai, llm_ubicloud)


def process_commits(repo_path, repo_name, provider, override):
    os.system(
        f"git -C {repo_path} log -p -n 1000 --pretty=format:'COMMIT_HASH:%H|AUTHOR_NAME:%an|AUTHOR_EMAIL:%ae|DATE:%ad|TITLE:%s|MESSAGE:%b' --date=iso > commit_data.txt"
    )

    with open('commit_data.txt', 'r') as file:
        lines = file.readlines()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if not override:
                cur.execute(
                    """SELECT "id" FROM commits WHERE "repo" = %s""", (repo_name,))
            else:
                cur.execute(
                    """SELECT "id" FROM commits WHERE "repo" = %s AND updated_at > %s""", (repo_name, override))
            processed_commit_ids = {row[0] for row in cur.fetchall()}
    finally:
        release_db_connection(conn)

    commit_data_list = []
    commit_id = author_name = author_email = commit_date = title = message = ""
    changes_list = []
    in_diff_section = False

    def maybe_save_commit():
        if commit_id and commit_id not in processed_commit_ids:
            changes = "\n".join(changes_list)
            author = f"{author_name} <{author_email}>"
            commit_data_list.append(
                (repo_name, commit_id, author, commit_date,
                 changes, title, message, provider)
            )

    for line in lines:
        line = line.strip()
        if line.startswith("COMMIT_HASH:"):
            maybe_save_commit()
            commit_id = line.split("COMMIT_HASH:")[1].strip()
            in_diff_section = False
            changes_list = []
        elif line.startswith("AUTHOR_NAME:"):
            author_name = line.split("AUTHOR_NAME:")[1].strip()
        elif line.startswith("AUTHOR_EMAIL:"):
            author_email = line.split("AUTHOR_EMAIL:")[1].strip()
        elif line.startswith("DATE:"):
            commit_date = line.split("DATE:")[1].strip()
        elif line.startswith("TITLE:"):
            title = line.split("TITLE:")[1].strip()
        elif line.startswith("MESSAGE:"):
            message = line.split("MESSAGE:")[1].strip()
        elif line == "" and commit_id:
            in_diff_section = True
        elif in_diff_section:
            changes_list.append(line)

    maybe_save_commit()

    print(f"Processing {len(commit_data_list)} commits in parallel...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_commit, *commit_data)
            for commit_data in commit_data_list
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing a commit: {e}")

    if os.path.exists('commit_data.txt'):
        os.remove('commit_data.txt')

    print("Commit processing complete.")


def main(repo_name, provider=None, override=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = """SELECT 1 FROM repos WHERE "name" = %s"""
            if not override:
                cur.execute(query, (repo_name,))
            else:
                query += " AND updated_at > %s"
                cur.execute(query, (repo_name, override))
            row = cur.fetchone()
            if row:
                print(
                    f"Repository '{repo_name}' already processed. Exiting...")
                return
    finally:
        release_db_connection(conn)

    repo_path = f"repos/{repo_name}"
    if not os.path.exists(repo_path):
        print(
            f"Repository '{repo_name}' not found at expected path {repo_path}. Exiting...")
        return

    print(f"Processing repository '{repo_name}'...")
    print("Processing folders and files...")
    for root, dirs, files in os.walk(repo_path, topdown=False):
        dirs[:] = [d for d in dirs if is_acceptable_folder(d)]
        process_folder(root, repo_path, repo_name, provider, override)

    print("Processing commits...")
    process_commits(repo_path, repo_name, provider, override)
    insert_repo(repo_name)
    backfill(repo_name, provider, override)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Process a repository with optional provider and override options.")
    parser.add_argument("repo", help="The repository to process.")
    parser.add_argument(
        "--provider", choices=['openai', 'ubicloud'], help="Specify the provider.")
    parser.add_argument(
        "--override", help="Enable override mode, which accepts a timestamp for updated_at")

    args = parser.parse_args()
    main(args.repo, args.provider, args.override)

    connection_pool.closeall()
