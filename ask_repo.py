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


def query_files(repo, question, top_k=10):
    query = """
        SELECT "name", "description" 
        FROM files 
        WHERE repo = %s
        ORDER BY vector <-> openai_embedding(%s, %s)
        LIMIT %s
    """
    cur.execute(query, (repo, EMBEDDING_MODEL, question, top_k))
    files = cur.fetchall()
    return files


def ask_question(repo, question):
    files = query_files(repo, question)

    system_prompt = f"You are an expert on the code repo {repo}. You are asked questions, and provided context to help answer them. The context is the file name and description. Answer the question, using the context if and only if it is helpful"

    context = ""
    file_count = len(files)
    for i in range(file_count):
        name, description = files[i]
        context += f"FILE {i+1} / {file_count}: {name}\n{description}\n\n"
    if not context:
        return "No relevant information found to answer your question."
    user_prompt = f"QUESTION: {question}\nCONTEXT: {context}"

    query = """
        SELECT llm_completion(%s, %s, %s)
    """
    cur.execute(query, (user_prompt, COMPLETION_MODEL, system_prompt))
    answer = cur.fetchone()[0]
    return answer


if __name__ == "__main__":
    if len(sys.argv) == 3:
        repo_name = sys.argv[1]
        question = sys.argv[2]
        answer = ask_question(repo_name, question)
        print(answer)
    repo_name = sys.argv[1]
    question = sys.argv[2]
    answer = ask_question(repo_name, question)
    print(answer)

    # Close database connection
    cur.close()
    conn.close()
