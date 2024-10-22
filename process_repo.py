import os
import re
import argparse
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from pgconf_utils import ask_openai, ask_ubicloud, OPENAI_CONTEXT_WINDOW, UBICLOUD_CONTEXT_WINDOW
from dotenv import load_dotenv
from backfill_embeddings import backfill
load_dotenv()

MAX_WORKERS = 200

# Prompts
FILE_PROMPT = """Here is some code. Summarize what the code does."""
FILE_SUMMARIES_PROMPT = """Here are multiple summaries of sections of a file. Summarize what the code does."""
FOLDER_PROMPT = """Here are the summaries of the files and subfolders in this folder. Summarize what the folder does."""
FOLDER_SUMMARIES_PROMPT = """Here are multiple summaries of the files and subfolders in this folder. Summarize what the folder does."""
REPO_PROMPT = """Here are the summaries of the folders in this repository. Summarize what the repository does."""
COMMIT_PROMPT = """Here is a commit, including the commit message, and the changes made in the commit. Summarize the commit."""

# Queries
INSERT_COMMIT = """
    INSERT INTO commits ("repo", "id", "author", "date", "changes", "title", "message", "llm_openai", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("repo", "id") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai", "llm_ubicloud" = EXCLUDED."llm_ubicloud";
"""
INSERT_COMMIT_OPENAI = """
    INSERT INTO commits ("repo", "id", "author", "date", "changes", "title", "message", "llm_openai")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("repo", "id") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai";
"""
INSERT_COMMIT_UBICLOUD = """
    INSERT INTO commits ("repo", "id", "author", "date", "changes", "title", "message", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("repo", "id") DO UPDATE SET "llm_ubicloud" = EXCLUDED."llm_ubicloud";
"""
INSERT_FOLDER = """
    INSERT INTO folders ("name", "repo", "llm_openai", "llm_ubicloud")
    VALUES (%s, %s, %s, %s)
    ON CONFLICT ("name", "repo") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai", "llm_ubicloud" = EXCLUDED."llm_ubicloud";
"""
INSERT_FOLDER_OPENAI = """
    INSERT INTO folders ("name", "repo", "llm_openai")
    VALUES (%s, %s, %s)
    ON CONFLICT ("name", "repo") DO UPDATE SET "llm_openai" = EXCLUDED."llm_openai";
"""
INSERT_FOLDER_UBICLOUD = """
    INSERT INTO folders ("name", "repo", "llm_ubicloud")
    VALUES (%s, %s, %s)
    ON CONFLICT ("name", "repo") DO UPDATE SET "llm_ubicloud" = EXCLUDED."llm_ubicloud";
"""
INSERT_FILE = """
    INSERT INTO files ("name", "folder", "repo", "code", "llm_openai", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT ("name", "folder", "repo") DO UPDATE SET "code" = EXCLUDED."code", "llm_openai" = EXCLUDED."llm_openai", "llm_ubicloud" = EXCLUDED."llm_ubicloud";
"""
INSERT_FILE_OPENAI = """
    INSERT INTO files ("name", "folder", "repo", "code", "llm_openai")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("name", "folder", "repo") DO UPDATE SET "code" = EXCLUDED."code", "llm_openai" = EXCLUDED."llm_openai";
"""
INSERT_FILE_UBICLOUD = """
    INSERT INTO files ("name", "folder", "repo", "code", "llm_ubicloud")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("name", "folder", "repo") DO UPDATE SET "code" = EXCLUDED."code", "llm_ubicloud" = EXCLUDED."llm_ubicloud";
"""

# Database
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


def is_acceptable_file(file_name):
    ACCEPTABLE_SUFFIXES = [
        '.py', '.js', '.java', '.rb', '.go', '.rs', '.json',
        '.yaml', '.yml', '.xml', '.md', '.txt', '.sh', '.sql', '.ts', '.h', '.c', '.cpp', '.hpp', '.php', '.jsx', '.tsx', '.swift', '.kt', '.cs', '.out'
    ]
    ACCEPTABLE_FILENAMES = {'Makefile', 'Dockerfile', '.env'}
    REJECTED_FILENAMES = 'package-lock.json'

    if any(file_name.endswith(suffix) for suffix in REJECTED_FILENAMES):
        return False

    return (
        any(file_name.endswith(suffix) for suffix in ACCEPTABLE_SUFFIXES) or
        file_name in ACCEPTABLE_FILENAMES
    )


def is_acceptable_folder(folder_name):
    EXCLUDED_DIRS = {'.git', '.devcontainer', '.venv', 'node_modules', }
    path_parts = folder_name.split(os.sep)
    return not any(part in EXCLUDED_DIRS for part in path_parts)


def insert_repo(repo_name):
    INSERT_REPO = f"""INSERT INTO repos ("name") VALUES (%s) ON CONFLICT DO NOTHING;"""
    cur.execute(INSERT_REPO, (repo_name,))
    conn.commit()


def insert_folder(folder_name, repo_name, llm_openai, llm_ubicloud):
    if llm_openai and llm_ubicloud:
        cur.execute(INSERT_FOLDER, (folder_name, repo_name,
                                    llm_openai.strip(), llm_ubicloud.strip()))
    elif llm_openai:
        cur.execute(INSERT_FOLDER_OPENAI, (folder_name, repo_name,
                                           llm_openai.strip()))
    elif llm_ubicloud:
        cur.execute(INSERT_FOLDER_UBICLOUD, (folder_name, repo_name,
                                             llm_ubicloud.strip()))
    conn.commit()


def insert_file(file_name, folder_name, repo_name, file_content, llm_openai, llm_ubicloud):
    if llm_openai and llm_ubicloud:
        cur.execute(INSERT_FILE, (file_name, folder_name, repo_name,
                                  file_content, llm_openai.strip(), llm_ubicloud.strip()))
    elif llm_openai:
        cur.execute(INSERT_FILE_OPENAI, (file_name, folder_name, repo_name,
                                         file_content, llm_openai.strip()))
    elif llm_ubicloud:
        cur.execute(INSERT_FILE_UBICLOUD, (file_name, folder_name, repo_name,
                                           file_content, llm_ubicloud.strip()))
    conn.commit()


def insert_commit(repo_name, commit_id, author, date, changes, title, message, llm_openai, llm_ubicloud):
    if llm_openai and llm_ubicloud:
        cur.execute(INSERT_COMMIT, (repo_name, commit_id, author, date,
                                    changes, title, message, llm_openai.strip(), llm_ubicloud.strip()))
    elif llm_openai:
        cur.execute(INSERT_COMMIT_OPENAI, (repo_name, commit_id, author, date,
                                           changes, title, message, llm_openai.strip()))
    elif llm_ubicloud:
        cur.execute(INSERT_COMMIT_UBICLOUD, (repo_name, commit_id, author, date,
                                             changes, title, message, llm_ubicloud.strip()))
    conn.commit()


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
    file_name = os.path.basename(file_path)

    # If file already has a summary, skip processing and just return it
    if not override:
        cur.execute(
            """SELECT "llm_openai", "llm_ubicloud" FROM files WHERE "name" = %s AND "folder" = %s AND "repo" = %s""", (file_name, folder_name, repo_name))
        row = cur.fetchone()
        if row:
            return row

    # Summarize each chunk and combine summaries
    def get_description(chunks, ask):
        if len(chunks) == 1:
            return ask(FILE_PROMPT + "\n\nFile: " + file_name + "\n\n" + chunks[0])
        else:
            descriptions = []
            for chunk in chunks:
                descriptions.append(
                    ask(FILE_PROMPT + "\n\nFile: " + file_name + "\n\n" + chunk))
            return ask(FILE_SUMMARIES_PROMPT + "\n\nFile: " + file_name + "\n\n" + "\n".join(descriptions[:10]))

    print("File:", file_path)
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        file_content = f.read()

        llm_ubicloud = llm_openai = None

        if provider == None or provider == 'openai':
            chunks_openai = chunk_file(file_content, OPENAI_CONTEXT_WINDOW)
            llm_openai = get_description(chunks_openai, ask_openai)
        if provider == None or provider == 'ubicloud':
            chunks_ubicloud = chunk_file(file_content, UBICLOUD_CONTEXT_WINDOW)
            llm_ubicloud = get_description(chunks_ubicloud, ask_ubicloud)

        # Insert the file and its components into the database
        insert_file(file_name, folder_name, repo_name,
                    file_content, llm_openai, llm_ubicloud)

        return llm_openai, llm_ubicloud


def process_files_in_folder(folder_path, repo_path, repo_name, provider, override):
    """
    Processes all acceptable files in a given folder concurrently.
    """
    file_futures = []
    folder_name = os.path.relpath(folder_path, repo_path)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path) and is_acceptable_file(item):
                # Submit file processing as a task
                file_futures.append(executor.submit(
                    process_file, item_path, folder_name, repo_name, provider, override))

        # Wait for all file processing tasks to complete
        summaries = []
        for future in as_completed(file_futures):
            try:
                llm_openai, llm_ubicloud = future.result()
                summaries.append((llm_openai, llm_ubicloud))
            except Exception as e:
                print(f"Error processing file in folder '{folder_name}': {e}")

    return summaries


def get_description(descriptions, ask, context_window):
    max_descriptions = int(context_window / 450)
    if len(descriptions) < max_descriptions:
        return ask(FOLDER_PROMPT + "\n\n" + "\n".join(descriptions))
    else:
        combined_descriptions = []
        for i in range(0, min(len(descriptions),  max_descriptions * max_descriptions), max_descriptions):
            combined_description = ask(
                FOLDER_PROMPT + "\n\n" + "\n".join(descriptions[i:i+max_descriptions]))
            combined_descriptions.append(combined_description)
        return ask(FOLDER_SUMMARIES_PROMPT + "\n\n" + "\n".join(combined_descriptions))


def process_folder(folder_path, repo_path, repo_name, provider, override):
    if not is_acceptable_folder(folder_path):
        return

    folder_name = os.path.relpath(folder_path, repo_path)
    print(f"Processing folder: {folder_name}")

    # If folder already has a summary, skip processing and just return it
    if not override:
        cur.execute(
            """SELECT "llm_openai", "llm_ubicloud" FROM folders WHERE "name" = %s AND "repo" = %s""", (folder_name, repo_name))
        row = cur.fetchone()
        if row:
            return [row]

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
    if len(changes) < min(OPENAI_CONTEXT_WINDOW, UBICLOUD_CONTEXT_WINDOW):
        input_text = f"{message}\nChanges: {changes}\nAuthor: {author_email}\nDate: {date}"
    else:
        files_changed = extract_files_changed(changes)
        input_text = f"{message}\nFiles changed: {', '.join(files_changed)}\nAuthor: {author_email}\nDate: {date}"

    llm_ubicloud = llm_openai = None
    if provider == None or provider == 'openai':
        llm_openai = ask_openai(COMMIT_PROMPT + "\n\n" + input_text)
    if provider == None or provider == 'ubicloud':
        llm_ubicloud = ask_ubicloud(COMMIT_PROMPT + "\n\n" + input_text)

    insert_commit(repo_name, commit_id, author, date,
                  changes, title, message, llm_openai, llm_ubicloud)


def process_commits(repo_path, repo_name, provider, override):
    # Extract commit data using git log
    os.system(
        f"git -C {repo_path} log -p -n 1000 --pretty=format:'COMMIT_HASH:%H|AUTHOR_NAME:%an|AUTHOR_EMAIL:%ae|DATE:%ad|TITLE:%s|MESSAGE:%b' --date=iso > commit_data.txt"
    )

    # Read commit data from file
    with open('commit_data.txt', 'r') as file:
        lines = file.readlines()

    # Previously processed commit IDs
    processed_commit_ids = {}
    if not override:
        cur.execute(
            """SELECT "id" FROM commits WHERE "repo" = %s""", (repo_name,))
        processed_commit_ids = {row[0] for row in cur.fetchall()}

    # Variables to store commit data
    commit_id = author_name = author_email = commit_date = title = message = ""
    changes_list = []
    in_diff_section = False

    # Store commit data for parallel processing
    commit_data_list = []

    def maybe_save_commit():
        if commit_id and commit_id not in processed_commit_ids:
            changes = "\n".join(changes_list)
            author = f"{author_name} <{author_email}>"
            commit_data_list.append(
                (repo_name, commit_id, author, commit_date, changes, title, message, provider))

    # Process each line to extract commit data
    for line in lines:
        line = line.strip()

        if line.startswith("COMMIT_HASH:"):
            maybe_save_commit()
            # Start reading a new commit
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
            # Empty line indicates the start of the diff section
            in_diff_section = True
        elif in_diff_section:
            # Accumulate the diff (changes)
            changes_list.append(line)

    # Insert the last commit's data
    maybe_save_commit()

    # Process commits in parallel using ThreadPoolExecutor
    print(f"Processing {len(commit_data_list)} commits in parallel...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_commit, *commit_data)
            for commit_data in commit_data_list
        ]
        for future in as_completed(futures):
            try:
                future.result()  # Raise any exceptions that occurred during processing
            except Exception as e:
                print(f"Error processing a commit: {e}")

    # Delete the temporary commit data file
    if os.path.exists('commit_data.txt'):
        os.remove('commit_data.txt')

    print("Commit processing complete.")


def main(repo_name, provider=None, override=False):
    # Check if the repository has already been processed
    cur.execute(
        """SELECT "name" FROM repos WHERE "name" = %s""", (repo_name,))
    row = cur.fetchone()
    if row:
        print(f"Repository '{repo_name}' already processed. Exiting...")
        return

    # Validate the correct repo path
    repo_path = f"repos/{repo_name}"
    if not os.path.exists(repo_path):
        print(
            f"Repository '{repo_name}' not found at expected path {repo_path}. Exiting...")
        return
    print(f"Processing repository '{repo_name}'...")

    # Walk through the directory tree
    print("Processing folders and files...")
    for root, dirs, files in os.walk(repo_path, topdown=False):
        dirs[:] = [d for d in dirs if is_acceptable_folder(d)]
        process_folder(root, repo_path, repo_name, provider, override)

    # Process commits
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
    parser.add_argument("--override", action='store_true',
                        help="Enable override mode.")

    args = parser.parse_args()

    main(args.repo, args.provider, args.override)

    cur.close()
    conn.close()
