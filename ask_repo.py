import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
COMPLETION_MODEL = "openai/gpt-4o-mini"
EMBEDDING_MODEL = "openai/text-embedding-3-small"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


def query_files(repo, question, top_k=5):
    query = """
        SET lantern_extras.openai_token = %s;
        SELECT "name", "code", "description" 
        FROM files 
        WHERE repo = %s
        ORDER BY vector <-> openai_embedding(%s, %s)
        LIMIT %s
    """
    cur.execute(query, (OPENAI_KEY, repo, EMBEDDING_MODEL, question, top_k))
    files = cur.fetchall()
    return files


def ask(question, context) -> str:
    query = """
        SET lantern_extras.openai_token = %s;
        SELECT openai_completion(%s, %s, %s)
    """
    cur.execute(query, (OPENAI_KEY, question, COMPLETION_MODEL, context))
    answer = cur.fetchone()[0]
    return answer


def main(repo: str, question: str) -> str:
    print("QUESTION")
    print(question)

    files = query_files(repo, question)

    print("\nRELEVANT FILES")
    context = ""
    for file in files:
        name, code, description = file
        print('-', name)
        context += f"File: {name}\nDescription: {description}\n\n"
    if not context:
        return "No relevant information found to answer your question."
    context = f"Answer user questions using the following context.\n\nContext:\n{context}\n"

    answer = ask(question, context)
    print("\nANSWER")
    print(answer)


if len(sys.argv) != 3:
    print("Usage: python qa_rag.py <repo> <question>")
    sys.exit(1)


if __name__ == "__main__":
    repo_name = sys.argv[1]
    question = sys.argv[2]
    main(repo_name, question)

    # Close database connection
    cur.close()
    conn.close()
