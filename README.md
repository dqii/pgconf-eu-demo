# Lantern + Ubicloud Demo for PGConf EU

In this demo, we'll be using Lantern and Ubicloud to build a simple chatbot that can answer questions about the Ubicloud codebase. Feel free to substitute the Ubicloud repository with your own codebase to build your own codebase expert.

## Step 1: Setup

Set the following environment variables in your `.env` file:

- `DATABASE_URL`
- `OPENAI_API_KEY`
- `UBICLOUD_API_KEY`

Next, load the environment variables

```bash
export $(cat .env | xargs)
```

Then, connect to the database

```bash
psql "$DATABASE_URL"
```

Next, configure the Lantern environment

```sql
ALTER SYSTEM SET lantern_extras.enable_daemon=true;
SELECT pg_reload_conf();
```

Set the database environment variables

```bash
psql "$DATABASE_URL" -c "ALTER DATABASE postgres SET lantern_extras.openai_token='$OPENAI_API_KEY'"
```

## Step 2: Database schema

Run the following command to create the database schema:

```sql
create table files (
    id serial primary key,
    repo text,
    name text,
    code text
);
```

## Step 3: Load the data

First, we'll clone the repo that we want to ask questions about:

```bash
mkdir -p repos
gh repo clone ubicloud/ubicloud repos/ubicloud
```

Then, we'll run the following command to load the files into the database:

```bash
python process_repo.py ubicloud
```

## Step 4: Initialize the LLM completion job and embedding generation job

Initialize the LLM completion job:

```sql
-- OpenAI
SELECT add_completion_job(
    'files',
    'code',
    'description',
    'Summarize this code'
);

-- TODO: Ubicloud
-- SELECT add_completion_job('files', 'code', 'description', 'Summarize this code', 'TEXT', 'llama-3-2-3b-it', 100, runtime_params=>'{"base_url": "https://e5-mistral-7b-it.ai.ubicloud.com"}');
```

Initialize the embedding generation job:

```sql
-- OpenAI
SELECT add_embedding_job(
    'files',                         -- table
    'description',                   -- source column
    'vector',                        -- output column
    'openai/text-embedding-3-small', -- model
    'openai'                         -- provider
);

-- Ubicloud
SELECT add_embedding_job('files', 'description', 'vector', 'e5-mistral-7b-it', 'openai', runtime_params=>'{"base_url": "https://llama-3-2-3b-it.ai.ubicloud.com"}');
```

## Step 5: Look at our data

```bash
psql "$DATABASE_URL" -c "SELECT name, description FROM files LIMIT 5"
psql "$DATABASE_URL" -c "SELECT name, vector FROM files LIMIT 5"
```

## Step 6: Run the chatbot to ask questions

```bash
python app.py ubicloud
```
