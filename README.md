# Sparkify Database - Schema and ETL

## Overview

This project provides the schema and ETL to create and populate an analytics database for the music streaming app Sparkify.

It has been designed as a PostgreSQL relational database in a star schema, which allows the Sparkify team to readily run queries to analyze user activity on their app, such as on what songs users are listening to. The scripts have been created in Python, leveraging its convenient wrapper around Postgres.

## Structure

The project contains the following elements:
* `data/` contains song and log files on user activity in JSON format
* `sql_queries.py` defines the SQL queries that underpin the creation of the database schema and ETL pipeline
* `create_tables.py` creates the Sparkify database and tables
* `etl.py` defines the ETL pipeline, which pulls and transforms the song and log JSON files in the local directory and inserts them into the Postgres database
* `etl.ipynb` and `test.ipynb` sketch out and test some of the elements contained in the Python scripts

## Schema

The database contains the following fact table:
* `songplays` - user song plays

`songplays` has foreign keys to the following(self-explanatory) dimension tables:
* `users`
* `songs`
* `artists`
* `time`

## Instructions

To run the project in Udacity's Project Workspace, open IPython and enter the following:

```
run create_tables.py
run etl.py
```

Make sure your working directory is at the top-level of the project.

## Query Example

Once you've created the database and run the ETL pipeline, you can test out some queries:

```
# Connect to database
%load_ext sql
%sql postgresql://student:student@127.0.0.1/sparkifydb

# Number of song plays before Nov 15, 2018
%sql SELECT COUNT(*) FROM songplays WHERE start_time < '2018-11-15';

# Distinct artists among song plays
%sql SELECT DISTINCT a.artist_name \
FROM songplays s \
LEFT JOIN artists a ON s.artist_id = a.artist_id;
```
