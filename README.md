# Databricks Table Editor

> **Disclaimer:** This is a personal project and is not affiliated with, endorsed by, or associated with Databricks, Inc. or my employment there. All opinions and code are my own.

A Streamlit app that lets you query a Databricks table, edit rows in-browser, and write changes back via SQL `UPDATE` statements.

## How it works

1. Configure your Databricks connection and target table in `config.yaml`.
2. Run a SQL query to load data into an editable table.
3. Edit cells directly in the browser.
4. Click **Save Changes** to push updates back to Databricks.

## Important: Unique Row ID Requirement

Each table **must** have a column with unique values per row (e.g. a primary key or `id` column). This column is specified as `unique_row_id` in `config.yaml` and is used to build the `WHERE` clause for each `UPDATE` statement. Without a unique key, the app cannot reliably target individual rows for updates.

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
2. Fill in `config.yaml` with your Databricks host, SQL warehouse HTTP path, PAT token, catalog, schema, table, and unique row ID column.
3. Run the app:
   ```
   streamlit run app.py
   ```
