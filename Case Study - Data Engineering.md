Ebury What borders?

## Case Study

Senior Data Engineer (Platform)

[info@ebury.ca |](mailto:info@ebury.ca)

ebury.ca


## Take home task

## Objective

Develop a data pipeline using Airflow, dbt, and PostgreSQL to ingest, transform, and expose data. The pipeline should be containerized and deployable using Docker Compose. The goal is to demonstrate the candidate's proficiency in data engineering workflows.

## Task Description

- 1. Data ingestion and preparation

- Use the provided dataset (customer_transactions.csv) and load it into a PostgreSQL database.

- Ensure the data is loaded efficiently, considering memory usage and performance.

- 2. Data transformation and aggregation

- Use dbt to clean and transform the raw data, creating one dimension table (`dim_table`) and one fact table (`fact_table`) based on the dataset provided.

- Transformations should address any data cleaning, and formatting.

- Standardize relevant fields, and ensure that dates, numeric fields, and identifiers are processed in a way that supports downstream use cases.

- Summarize the data to provide insights at an aggregate level, such as monthly totals or totals by customer. Consider how these summaries could be useful and what information should be included.

- Set up Airflow to create a DAG that:

- i. Downloads and ingests the raw data.

- ii. Triggers dbt to process and transform the data into the dimensional model.

- iii. Optionally includes task dependencies, retries, or notification logic if time allows.

## 3. Data orchestration


## Take home task

Please notice this role is platform focused, building frameworks and applying best practices so you should consider a high-level approach that prioritizes best practices and scalability over individual pipeline specifics. There is no need to implement everything but to demonstrate that these topics are considered.

- Design and implement data quality checks to handle common issues, such as missing or inconsistent data. Think about what types of issues might compromise data quality and document your approach.

- Where possible, implement mechanisms to log or flag issues and address them in the data.

Other relevant topics are optimisation, automation, data governance, etc.

- Use Docker and `docker-compose` to manage PostgreSQL, Airflow and dbt.

- PostgreSQL for raw and processed data storage.

- dbt for data transformations and model creation.

- Airflow for orchestration.

A well-organized GitHub repository including:

- A `README.md` with clear setup instructions and a brief description of the pipeline.

- A `docker-compose.yml` file orchestrating all services (PostgreSQL, Airflow and dbt).

- Any required dbt files, Airflow DAGs, and SQL scripts.

- The entire pipeline should be deployable with `docker-compose up`.

## About

## Data quality

## Technology stack

## Output - Repository structure

## Submission instructions

Please complete this task before our scheduled call.

Share a GitHub repository link containing all required files and documentation replying to the email invitation please.

Please, remember there’s no single right answer; we’re eager to learn about your thought process and share our experiences together. Looking forward to our conversation!


## Take home task

Schema for customer_transactions.csv

| Field | Data type | Description |
| --- | --- | --- |
|   | (expected) |   |
| transaction_id | integer | Unique identifier for each transaction |
| customer_id | integer | Unique identifier for each customer |
| transaction_date | date | Date when the transaction occurred |
| product_id | Integer | Unique identifier for each product |
| product_name | String | Name of the product |
| quantity | Integer | Number of units sold |
| price | Float | Price of the product |
| tax | Float | Tax applied to the transaction |


Ebury

info@ebury.com | ebury.com [URL 🔗](mailto:info@ebury.com)

Ebury Partners Belgium NV/SA is authorised and regulated by the National Bank of Belgium as a Payment Institution under the Act of 11 March 2018, registered with the Crossroads Bank for Enterprises under number 0681.746.187. EBURY and EBURY What Borders? are trademarks.

© Copyright 2009-2022
