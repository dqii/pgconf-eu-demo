import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()

# Database
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

query = """
-- migrate:up
create extension if not exists vector;

alter system set lantern_extras.enable_daemon=true;
select pg_reload_conf();

create table if not exists repos (
    "name" text primary key
);

create table if not exists folders (
    "id" serial primary key,
    "repo" text,
    "name" text,
    "llm_input_openai" text,
    "llm_input_ubicloud" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    "vector_openai" vector(1536),
    "vector_ubicloud" vector(4096),
    unique ("name", "repo")
);

create table if not exists files (
    "id" serial primary key,
    "repo" text,
    "folder" text,
    "name" text,
    "code" text,
    "llm_input_openai" text,
    "llm_input_ubicloud" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    "vector_openai" vector(1536),
    "vector_ubicloud" vector(4096),
    unique ("name", "folder", "repo")
);

create table if not exists file_parts (
    id serial primary key,
    file_id integer references files(id),
    "repo" text,
    "folder" text,
    "name" text,
    "part" integer,
    "llm_input" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    unique ("repo", "folder", "name")
);

create table if not exists commits (
    "id" serial primary key,
    "repo" text,
    "commit_id" text,
    "author" text,
    "date" text,
    "changes" text,
    "message" text,
    "llm_input" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    "vector_openai" vector(1536),
    "vector_ubicloud" vector(4096),
    unique ("repo", "commit_id")
);


SELECT add_completion_job('files', 'llm_input', 'llm_openai', 'Summarize this code', 'TEXT', 'openai/gpt-4o-mini', 128000);
SELECT add_completion_job('files', 'llm_input', 'llm_ubicloud', 'Summarize this code', 'TEXT', 'openai/gpt-4o-mini', 90000);

SELECT add_completion_job('file_parts', 'llm_input', 'llm_openai', 'Summarize this code', 'TEXT', 'openai/gpt-4o-mini', 128000);
SELECT add_completion_job('file_parts', 'llm_input', 'llm_ubicloud', 'Summarize this code', 'TEXT', 'openai/gpt-4o-mini', 90000);

SELECT add_completion_job('folders', 'llm_input', 'llm_openai', 'Summarize this code', 'TEXT', 'openai/gpt-4o-mini', 128000);
SELECT add_completion_job('folders', 'llm_input', 'llm_ubicloud', 'Summarize this code', 'TEXT', 'openai/gpt-4o-mini', 90000);

select add_embedding_job('folders', 'llm_input_openai', 'llm_openai', 'openai/text-embedding-3-small', 'openai');
select add_embedding_job('folders', 'llm_input_ubicloud', 'llm_ubicloud', 'openai/text-embedding-3-small', 'openai');

select add_embedding_job('files', 'llm_input_openai', 'llm_openai', 'openai/text-embedding-3-small', 'openai');
select add_embedding_job('files', 'llm_input_ubicloud', 'llm_ubicloud', 'openai/text-embedding-3-small', 'openai');

select add_embedding_job('commits', 'llm_input', 'llm_openai', 'openai/text-embedding-3-small', 'openai');
select add_embedding_job('commits', 'llm_input', 'llm_ubicloud', 'openai/text-embedding-3-small', 'openai');

-- migrate:down

drop table files;
drop table folders;
drop table repos;
drop table commits;
"""

cur.execute(query)
conn.commit()

cur.close()
conn.close()
