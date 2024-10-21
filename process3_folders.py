import os
import sys
import time
import psycopg2
from pgconf_utils import ask_openai, ask_ubicloud, OPENAI_CONTEXT_WINDOW, UBICLOUD_CONTEXT_WINDOW
from dotenv import load_dotenv
from process1_commits_and_file_parts import is_acceptable_folder
load_dotenv()

FOLDER_LONG_PROMPT = """Here are the summaries of the files and subfolders in this folder. Summarize what the folder does."""
FOLDER_SHORT_PROMPT = """Here are a subset of the files and subholders in this folder does. Summarize what the folder does"""


# Database
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


def insert_repo(repo_name):
    INSERT_REPO = f"""INSERT INTO repos ("name") VALUES (%s) ON CONFLICT DO NOTHING;"""
    cur.execute(INSERT_REPO, (repo_name,))
    conn.commit()


def insert_folder(folder_name, repo_name, llm_input_openai, llm_input_ubicloud):
    INSERT_FOLDER = """
        INSERT INTO folders ("name", "repo", "llm_input_openai", "llm_input_ubicloud")
        VALUES (%s, %s, %s, %s) ON CONFLICT ("name", "repo") DO NOTHING;
    """
    cur.execute(INSERT_FOLDER, (folder_name, repo_name,
                llm_input_openai.strip(), llm_input_ubicloud.strip()))
    conn.commit()


def process_folder(folder_path, repo_path, repo_name):
    if not is_acceptable_folder(folder_path):
        return
    print("Folder:", folder_path)

    # If folder already has a summary, skip processing and just return
    cur.execute(
        """SELECT 1 FROM folders WHERE "name" = %s AND "repo" = %s""", (folder_path, repo_name))
    row = cur.fetchone()
    if row:
        return

    # Full relative folder path
    folder_name = os.path.relpath(folder_path, repo_path)

    cur.execute("""
        SELECT name, llm_ubicloud, llm_openai FROM folders
        WHERE repo = %s AND folder = %s
        """, (repo_name, folder_name))
    folders = cur.fetchall()[:200]

    cur.execute("""
        SELECT name, llm_ubicloud, llm_openai FROM files
        WHERE repo = %s AND folder = %s
        """, (repo_name, folder_name))
    files = cur.fetchall()[:(200 - len(folders))]

    llm_input_openai = []
    llm_input_ubicloud = []
    for (folder, llm_ubicloud, llm_openai) in folders:
        llm_input_openai.append('FOLDER: ' + folder)
        llm_input_ubicloud.append('FOLDER: ' + folder)

        llm_input_openai.append('SUMMARY: ' + llm_openai)
        llm_input_ubicloud.append('SUMMARY: ' + llm_ubicloud)

        llm_input_openai.append('---------')
        llm_input_ubicloud.append('---------')

    for (file, llm_ubicloud, llm_openai) in files:
        llm_input_openai.append('FILE: ' + file)
        llm_input_ubicloud.append('FILE: ' + file)

        llm_input_openai.append('SUMMARY: ' + llm_openai)
        llm_input_ubicloud.append('SUMMARY: ' + llm_ubicloud)

        llm_input_openai.append('---------')
        llm_input_ubicloud.append('---------')

    insert_folder(folder_name, repo_name, '\n'.join(
        llm_openai), '\n'.join(llm_ubicloud))


def main(repo_name):
    # Check if the repository has already been processed
    cur.execute("""SELECT "name" FROM repos WHERE "name" = %s""", (repo_name,))
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

    # Check if the LLM completion jobs are done
    while True:
        cur.execute(
            """SELECT COUNT(*) FROM file_parts WHERE repo = %s AND (llm_ubicloud IS NULL OR llm_openai IS NULL)""")
        count = cur.fetchone()[0]
        if count == 0:
            break
        print(
            f"LLM completion job still running, {count} null columns remaining...")
        time.sleep(30)

    # Walk through the directory tree
    print("Processing folders and files...")
    for root, dirs, files in os.walk(repo_path, topdown=False):
        dirs[:] = [d for d in dirs if is_acceptable_folder(d)]
        process_folder(root, repo_path, repo_name)

    insert_repo(repo_name)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
        cur.close()
        conn.close()
    else:
        print("Usage: python process_repo.py <repo>")
