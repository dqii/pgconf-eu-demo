-- migrate:up
create extension if not exists vector;

create table if not exists repos (
    "name" text primary key
);

create table if not exists folders (
    "repo" text,
    "name" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    "vector_openai" vector(1536),
    "vector_ubicloud" vector(4096),
    primary key ("name", "repo")
);

create table if not exists files (
    "repo" text,
    "folder" text,
    "name" text,
    "code" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    "vector_openai" vector(1536),
    "vector_ubicloud" vector(4096),
    primary key ("name", "folder", "repo")
);

create table if not exists commits (
    "repo" text,
    "id" text,
    "author" text,
    "date" text,
    "changes" text,
    "message" text,
    "llm_openai" text,
    "llm_ubicloud" text,
    "vector_openai" vector(1536),
    "vector_ubicloud" vector(4096),
    primary key ("repo", "id")
);

-- migrate:down

drop table files;
drop table folders;
drop table repos;
drop table commits;