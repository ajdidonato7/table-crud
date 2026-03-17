import json
import streamlit as st
import pandas as pd
import yaml
from databricks import sql as databricks_sql
from pathlib import Path


def load_config():
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)["databricks"]


def get_connection(config):
    return databricks_sql.connect(
        server_hostname=config["host"],
        http_path=config["http_path"],
        access_token=config["token"],
    )


def run_query(config, query, catalog=None, schema=None):
    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            if catalog:
                cursor.execute(f"USE CATALOG `{catalog}`")
            if schema:
                cursor.execute(f"USE SCHEMA `{schema}`")
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def execute_sql(config, statements, catalog=None, schema=None):
    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            if catalog:
                cursor.execute(f"USE CATALOG `{catalog}`")
            if schema:
                cursor.execute(f"USE SCHEMA `{schema}`")
            for stmt in statements:
                cursor.execute(stmt)


def get_column_types(config, catalog, schema, table):
    try:
        df = run_query(config, f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table}`")
        return dict(zip(df["col_name"], df["data_type"]))
    except Exception:
        return {}


def format_value(val, col_type=""):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    escaped = str(val).replace("'", "''")
    if col_type.lower().strip() == "variant":
        try:
            json.loads(str(val))
            return f"PARSE_JSON('{escaped}')"
        except (json.JSONDecodeError, ValueError):
            return f"'{escaped}'"
    return f"'{escaped}'"


# --- Streamlit UI ---

st.set_page_config(page_title="Databricks Table Editor", layout="wide")
st.title("Databricks Table Editor")

config = load_config()

if not config.get("host") or not config.get("token") or not config.get("http_path"):
    st.error("Fill in your Databricks connection details in `config.yaml` before running.")
    st.stop()

catalog = config.get("catalog", "main")
schema = config.get("schema", "default")
row_id_col = config.get("unique_row_id", "")
table = config.get("table", "")

if not row_id_col:
    st.error("Set `unique_row_id` in `config.yaml` to the column that uniquely identifies each row.")
    st.stop()

if not table:
    st.error("Set `table` in `config.yaml` to the table name to write updates back to.")
    st.stop()

if "editor_key" not in st.session_state:
    st.session_state["editor_key"] = 0

# --- Sidebar inputs ---
with st.sidebar:
    st.header("Configuration")
    st.text(f"Catalog: {catalog}")
    st.text(f"Schema: {schema}")
    st.text(f"Table: {table}")
    st.text(f"Row ID column: {row_id_col}")
    st.divider()
    query = st.text_area(
        "SQL Query",
        value=f"SELECT * FROM `{table}`",
        help=f"Uses catalog `{catalog}` and schema `{schema}` automatically. Must include `{row_id_col}` column in results.",
    )
    load_btn = st.button("Run Query", type="primary", disabled=not query.strip())

# --- Load ---
if load_btn:
    with st.spinner("Querying Databricks..."):
        try:
            df = run_query(config, query.strip(), catalog=catalog, schema=schema)

            if df.empty:
                st.warning("Query returned no rows.")
                st.stop()

            if row_id_col not in df.columns:
                st.error(f"Column `{row_id_col}` not found in query results. Available columns: {list(df.columns)}")
                st.stop()

            col_types = get_column_types(config, catalog, schema, table)

            st.session_state["original_df"] = df.copy()
            st.session_state["col_types"] = col_types
            st.session_state["editor_key"] += 1
        except Exception as e:
            st.error(f"Query failed: {e}")

# --- Display & edit ---
if "original_df" in st.session_state:
    original_df = st.session_state["original_df"]
    col_types = st.session_state.get("col_types", {})
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    editable_cols = [c for c in original_df.columns if c != row_id_col]

    st.subheader(f"`{catalog}`.`{schema}`.`{table}`")
    st.caption(f"{len(original_df)} rows · Row ID: `{row_id_col}`")

    edited_df = st.data_editor(
        original_df,
        num_rows="dynamic",
        use_container_width=True,
        key=f"editor_{st.session_state['editor_key']}",
    )

    editor_state = st.session_state[f"editor_{st.session_state['editor_key']}"]
    added_rows = editor_state.get("added_rows", [])
    deleted_rows = editor_state.get("deleted_rows", [])

    if st.button("Save Changes", type="primary"):
        statements = []

        # --- Updates (existing rows that were modified) ---
        for idx in range(len(original_df)):
            if idx in deleted_rows:
                continue
            orig_row = original_df.iloc[idx]
            edit_row = edited_df.iloc[idx]
            rid = orig_row[row_id_col]

            set_clauses = []
            for col in editable_cols:
                if str(orig_row[col]) != str(edit_row[col]):
                    ct = col_types.get(col, "")
                    set_clauses.append(f"`{col}` = {format_value(edit_row[col], ct)}")

            if set_clauses:
                rid_type = col_types.get(row_id_col, "")
                where = f"`{row_id_col}` = {format_value(rid, rid_type)}"
                stmt = f"UPDATE {fqn} SET {', '.join(set_clauses)} WHERE {where}"
                statements.append(stmt)

        # --- Inserts (new rows added via the editor) ---
        for row_dict in added_rows:
            cols_with_values = {c: v for c, v in row_dict.items() if v is not None and str(v).strip() != ""}
            if not cols_with_values:
                continue
            col_names = ", ".join(f"`{c}`" for c in cols_with_values)
            col_vals = ", ".join(format_value(v, col_types.get(c, "")) for c, v in cols_with_values.items())
            stmt = f"INSERT INTO {fqn} ({col_names}) VALUES ({col_vals})"
            statements.append(stmt)

        # --- Deletes (rows removed via the editor) ---
        for idx in deleted_rows:
            rid = original_df.iloc[idx][row_id_col]
            rid_type = col_types.get(row_id_col, "")
            where = f"`{row_id_col}` = {format_value(rid, rid_type)}"
            stmt = f"DELETE FROM {fqn} WHERE {where}"
            statements.append(stmt)

        if not statements:
            st.info("No changes detected.")
        else:
            n_updates = sum(1 for s in statements if s.startswith("UPDATE"))
            n_inserts = sum(1 for s in statements if s.startswith("INSERT"))
            n_deletes = sum(1 for s in statements if s.startswith("DELETE"))
            summary = ", ".join(
                f"{n} {label}" for n, label in
                [(n_updates, "update"), (n_inserts, "insert"), (n_deletes, "delete")]
                if n > 0
            )
            with st.expander(f"SQL ({summary})", expanded=False):
                for s in statements:
                    st.code(s, language="sql")
            with st.spinner(f"Writing {len(statements)} statement(s)..."):
                try:
                    execute_sql(config, statements, catalog, schema)
                    st.success(f"Saved to Databricks: {summary}.")
                    st.session_state["original_df"] = edited_df.copy()
                    st.session_state["editor_key"] += 1
                    st.rerun()
                except Exception as e:
                    st.error(f"Update failed: {e}")
