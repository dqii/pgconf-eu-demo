import os
import re
import sys
import psycopg2
from pgconf_utils import CONTEXT_WINDOW
from dotenv import load_dotenv
load_dotenv()

FILE_PROMPT = """Here is the file {file} in the {repo} repo. Return an overall description of what the file does. If there are notable functions, include the function name, inputs, and return type in your description. Same goes for other significant components of the code."""
COMMIT_SHORT_PROMPT = """Here is a commit, including the commit message, the files changed, and the changes made in the commit. Summarize the purpose of the commit and the changes made, including file names and functions where appropriate."""
COMMIT_LONG_PROMPT = """Here is a commit, including the commit message, the files changed, and a subset of the changes made in the commit. Summarize the purpose of the commit and the changes made, including file names and functions where appropriate."""

FILE_PART_PROMPT = """Here is part of a file {file} in the {repo} repo. Return an overall description of what this part of the file does. If there are notable functions, include the function name, inputs, and return type in your description. Same goes for other significant components of the code."""

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
    return (
        any(file_name.endswith(suffix) for suffix in ACCEPTABLE_SUFFIXES) or
        file_name in ACCEPTABLE_FILENAMES
    )


def is_acceptable_folder(folder_name):
    EXCLUDED_DIRS = {'.git', '.devcontainer', '.venv', 'node_modules', }
    path_parts = folder_name.split(os.sep)
    return not any(part in EXCLUDED_DIRS for part in path_parts)


def insert_file(file_name, folder_name, repo_name, file_content, llm_openai, llm_ubicloud):
    INSERT_FILE = """
        INSERT INTO files ("name", "folder", "repo", "code", "llm_openai", "llm_ubicloud")
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT ("name", "folder", "repo") DO NOTHING;
    """
    cur.execute(INSERT_FILE, (file_name, folder_name, repo_name,
                file_content, llm_openai.strip(), llm_ubicloud.strip()))
    conn.commit()


def insert_file_part(file_name, folder_name, repo_name, part, llm_input):
    INSERT_FILE = """
        INSERT INTO file_parts ("name", "folder", "repo", "part", "llm_input")
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT ("name", "folder", "repo") DO NOTHING;
    """
    cur.execute(INSERT_FILE, (file_name, folder_name,
                repo_name, part, llm_input))
    conn.commit()


def insert_commit(repo_name, commit_id, author, date, changes, message, llm_input):
    INSERT_COMMIT = """
        INSERT INTO commits ("repo", "id", "author", "date", "changes", "message", "llm_input")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("repo", "id") DO NOTHING;
    """
    cur.execute(INSERT_COMMIT, (repo_name, commit_id, author, date,
                changes, message, llm_input))
    conn.commit()


def chunk_file(file_content, context_window):
    """
    Splits the file content into chunks, ensuring that each chunk ends at a function boundary.
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


def process_file(file_path, folder_name, repo_name):
    file_name = os.path.basename(file_path)

    # If file already has a summary, skip processing and just return it
    cur.execute(
        """SELECT 1 FROM file_parts WHERE "name" = %s AND "folder" = %s AND "repo" = %s""", (file_name, folder_name, repo_name))
    row = cur.fetchone()
    if row:
        return row

    print("File:", file_path)
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        file_content = f.read()
        chunks = chunk_file(file_content, CONTEXT_WINDOW)
        if len(chunks) == 0:
            llm_input = FILE_PROMPT.format(
                file=file_path, repo=repo_name) + '\n\n' + file_content
            insert_file_part(file_name, folder_name, repo_name, 0, llm_input)
        else:
            for i in range(len(chunks)):
                llm_input = FILE_PART_PROMPT.format(
                    file=file_path, repo=repo_name) + '\n\n' + file_content
                insert_file_part(file_name, folder_name,
                                 repo_name, i, llm_input)


def process_folder(folder_path, repo_path, repo_name):
    if not is_acceptable_folder(folder_path):
        return
    print("Folder:", folder_path)

    # Full relative folder path
    folder_name = os.path.relpath(folder_path, repo_path)

    # Process each file in the folder
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path) and is_acceptable_file(item):
            process_file(
                item_path, folder_name, repo_name)


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


def process_commits(repo_path, repo_name):
    # Extract commit data using git log
    os.system(
        f"git -C {repo_path} log -p -n 1000 --pretty=format:'COMMIT_HASH:%H|AUTHOR_NAME:%an|AUTHOR_EMAIL:%ae|DATE:%ad|TITLE:%s|MESSAGE:%b' --date=iso > commit_data.txt"
    )

    # Read commit data from file
    with open('commit_data.txt', 'r') as file:
        lines = file.readlines()

    # Previously processed commit IDs
    cur.execute("""SELECT "id" FROM commits WHERE "repo" = %s""", (repo_name,))
    processed_commit_ids = {row[0] for row in cur.fetchall()}

    # Variables to store commit data
    commit_id = author_name = author_email = commit_date = title = message = ""
    changes_list = []
    in_diff_section = False
    commit_count = 0

    def maybe_save_commit():
        nonlocal commit_count
        if commit_id in processed_commit_ids:
            return
        if commit_id:
            changes = "\n".join(changes_list)
            author = f"{author_name} <{author_email}>"
            prompt = COMMIT_LONG_PROMPT if len(
                changes) > CONTEXT_WINDOW else COMMIT_SHORT_PROMPT
            files_changed = extract_files_changed(changes)
            llm_input = '\n--------------\n'.join([prompt,
                                                   'TITLE:\n' + title,
                                                   'AUTHOR:\n' + author,
                                                   'FILES_CHANGED:\n' +
                                                   ', '.join(files_changed),
                                                   'DATE:\n' + commit_date,
                                                   'MESSAGE:\n' + message,
                                                   'CHANGES:\n' +
                                                   changes[:CONTEXT_WINDOW]
                                                   ])
            insert_commit(repo_name, commit_id, author, commit_date,
                          changes, message, llm_input)
            commit_count += 1
            if commit_count % 10 == 0:
                print(f"Processed {commit_count} commits...")

    # Process each line to extract and insert commit data
    for line in lines:
        line = line.strip()
        print(line)

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

    # Delete the temporary commit data file
    if os.path.exists('commit_data.txt'):
        os.remove('commit_data.txt')


def main(repo_name):
    # Check if the repository has already been processed
    cur.execute(
        """SELECT "name" FROM file_parts WHERE "name" = %s LIMIT 1""", (repo_name,))
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
        process_folder(root, repo_path, repo_name)

    # Process commits
    print("Processing commits...")
    process_commits(repo_path, repo_name)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
        cur.close()
        conn.close()
    else:
        print("Usage: python process_repo.py <repo>")
