import os
import sys
import time
import psycopg2
from dotenv import load_dotenv
load_dotenv()

FILE_AGGREGATE_PROMPT = """Here are the summaries of multiple parts of the file {file} in the {repo} repo. Return an overall description of what the file does. If there are notable functions, include the function name, inputs, and return type in your description. Same goes for other significant components of the code."""

# Database
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


def process_short_files(file_id_and_ids):
    for file_id, ids in file_id_and_ids:
        cur.execute("""
            UPDATE files f
            SET llm_ubicloud = fp.llm_ubicloud, llm_openai = fp.llm_openai
            FROM file_parts fp
            WHERE fp.file_id = f.id AND fp.id = ANY(%s) AND f.id = %s;
        """, (ids, file_id))
    conn.commit()


def process_long_files(file_id_and_ids):
    for file_id, ids in file_id_and_ids:
        cur.execute("""
            SELECT llm_ubicloud, llm_openai FROM file_parts WHERE id = ANY(%s)
        """, (ids,))
        rows = cur.fetchall()
        llm_ubiclouds = "\n\n".join(r[0] for r in rows if r[0])
        llm_openais = "\n\n".join(r[1] for r in rows if r[1])

        cur.execute("""
            UPDATE files
            SET llm_input_ubicloud = %s, llm_input_openai = %s
            WHERE id = %s;
        """, (llm_ubiclouds, llm_openais, file_id))

    conn.commit()


def main(repo_name):
    # Check if the repository has already been processed
    cur.execute(
        """SELECT 1 FROM "files" WHERE "repo" = %s LIMIT 1""", (repo_name,))
    row = cur.fetchone()
    if row:
        print(f"Repository '{repo_name}' already processed. Exiting...")
        return

    # Check if the LLM completion jobs are done
    while True:
        cur.execute("""
            SELECT COUNT(*) FROM file_parts WHERE repo = %s AND (llm_ubicloud IS NULL OR llm_openai IS NULL)
        """, (repo_name,))
        count = cur.fetchone()[0]
        if count == 0:
            break
        print(
            f"LLM completion job still running, {count} null columns remaining...")
        time.sleep(30)

    cur.execute("""
        SELECT file_id, ARRAY_AGG(id) FROM file_parts WHERE file_id IN (SELECT id FROM files WHERE repo = %s)""", (repo_name,))
    all_files = cur.fetchall()
    short_files = [(f[0], f[1]) for f in all_files if f[1] == 1]
    long_files = [(f[0], f[1]) for f in all_files if f[1] > 1]
    process_short_files(short_files)
    process_long_files(long_files)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
        cur.close()
        conn.close()
    else:
        print("Usage: python process_repo.py <repo>")
