# Lantern + Ubicloud Demo for PGConf EU

In this demo, we'll be using Lantern and Ubicloud to build a simple chatbot that can answer questions about the Ubicloud database. Feel free to substitute the Ubicloud repository with your own database to build your own repository chatbot.

## Step 1: Environment variables

Set the following environment variables in your `.env` file:

- `DATABASE_URL`
- `OPENAI_API_KEY`
- `UBICLOUD_API_KEY`

Then load the environment variables

```bash
export $(cat .env | xargs)
```

## Step 2: Database schema

First, connect to the database:

```bash
psql "$DATABASE_URL"
```

Then run the following command to create the database schema:

```sql
create table files (
    "id" serial primary key,
    "repo" text,
    "name" text,
    "code" text
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

As before, connect to the database:

```bash
psql "$DATABASE_URL"
```

Next, initialize the LLM completion job:

```sql
-- Ubicloud
SELECT 1;

-- OpenAI
```

Finally, initialize the embedding generation job:

```sql
-- Ubicloud
SELECT 1;

-- OpenAI
```

## Step 5: Run the chatbot

```bash
python app.py
```
