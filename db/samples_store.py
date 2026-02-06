# samples_store.py
import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

SAMPLES_DB_PATH = "samples.db"


def get_conn(db_path: str = SAMPLES_DB_PATH) -> sqlite3.Connection:
    """Open (and lazily initialize) the samples database."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    _init_schema(conn)
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist yet (no in-place migrations)."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extracted_samples (
            user_id   TEXT NOT NULL,
            pdf_id    TEXT NOT NULL,
            sample_id TEXT NOT NULL,
            parameter TEXT NOT NULL,
            unit      TEXT,
            value     TEXT,
            PRIMARY KEY (user_id, pdf_id, sample_id, parameter, unit)
        )
        """
    )

    # --- NEW TABLE FOR MAPPINGS ---
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS parameter_mappings (
            source_param TEXT PRIMARY KEY,
            target_eigenschap TEXT NOT NULL
        )
        """
    )

    conn.commit()


# Add this helper function at the top or inside the file
def clean_value_string(x):
    """
    Cleans a value string to make it numeric.
    Handles '<' signs (e.g. '<0.1' -> 0.1) and comma decimals ('0,5' -> 0.5).
    """
    if x is None:
        return None

    # If it's already a number, return as string or keep as is
    if isinstance(x, (int, float)):
        return str(x)

    s = str(x).strip()

    # 1. Handle less-than signs (common in lab results)
    # We treat "<0.1" as "0.1". (Adjust this logic if you need factor 0.7)
    s = s.replace("<", "").replace(">", "")

    # 2. Handle Dutch decimal commas
    s = s.replace(",", ".")

    return s

def save_sample_from_wide(
    conn: sqlite3.Connection,
    user_id: str,
    pdf_id: str,
    sample_id: str,
    wide_df: pd.DataFrame,
) -> None:
    """
    Take a 1-row wide dataframe (CBC-style), and store it as long form in extracted_samples.
    Columns SampleID and DateProcessed are not stored as parameters.
    """
    if wide_df.empty:
        return

    row = wide_df.iloc[0].to_dict()

    for param, value in row.items():
        if param in ("SampleID", "DateProcessed"):
            continue

        if value is None or (isinstance(value, float) and np.isnan(value)):
            cleaned_value = None
        else:
            cleaned_value = value

        conn.execute(
            """
            INSERT OR REPLACE INTO extracted_samples
                (user_id, pdf_id, sample_id, parameter, unit, value)
            VALUES (?, ?, ?, ?, NULL, ?)
            """,
            (user_id, pdf_id, sample_id, param, cleaned_value),
        )

    conn.commit()


def save_extraction_results(
    conn: sqlite3.Connection,
    user_id: str,
    pdf_id: str,
    results: pd.DataFrame,
) -> None:
    """
    Persist extracted results (long format) into the database.
    Expected columns: sample_id, parameter, unit, value.
    """
    if results.empty:
        return

    cols = {c.lower(): c for c in results.columns}
    required = ["sample_id", "parameter", "value"]
    for key in required:
        if key not in cols:
            raise ValueError(f"results missing required column '{key}'")

    for _, row in results.iterrows():
        sample = row[cols["sample_id"]]
        parameter = row[cols["parameter"]]
        unit = row.get(cols.get("unit"), None)
        value = row[cols["value"]]

        if pd.isna(unit):
            unit = None
        if pd.isna(value):
            value = None

        conn.execute(
            """
            INSERT OR REPLACE INTO extracted_samples
                (user_id, pdf_id, sample_id, parameter, unit, value)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, pdf_id, sample, parameter, unit, value),
        )

    conn.commit()


def get_parameter_mappings(conn: sqlite3.Connection) -> dict[str, str]:
    """Retrieve all custom mappings as {source_param: target_eigenschap}."""
    cur = conn.execute("SELECT source_param, target_eigenschap FROM parameter_mappings")
    return {row[0]: row[1] for row in cur.fetchall()}


def update_parameter_mapping(conn: sqlite3.Connection, source_param: str, target_eigenschap: str):
    """Insert or update a mapping."""
    if not source_param or not target_eigenschap:
        return

    # If the user selects "None" or empty, we might want to delete the mapping
    if target_eigenschap == "RESET":
        conn.execute("DELETE FROM parameter_mappings WHERE source_param = ?", (source_param,))
    else:
        conn.execute(
            "INSERT OR REPLACE INTO parameter_mappings (source_param, target_eigenschap) VALUES (?, ?)",
            (source_param, target_eigenschap)
        )
    conn.commit()

def load_sample_wide(
    conn: sqlite3.Connection,
    user_id: str,
    pdf_id: str,
    sample_id: str,
    required_cols: list[str],
) -> pd.DataFrame | None:
    """
    Reconstruct a wide 1-row dataframe for a given (user_id, pdf_id, sample_id)
    from extracted_samples. Returns None if no data is stored.
    """
    cur = conn.execute(
        """
        SELECT parameter, unit, value
        FROM extracted_samples
        WHERE user_id = ? AND pdf_id = ? AND sample_id = ?
        """,
        (user_id, pdf_id, sample_id),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    df_long = pd.DataFrame(rows, columns=["Parameter", "Unit", "Value"])

    # DEBUG PRINT
    print("DEBUG: Raw DB Rows sample:")
    print(df_long.head())

    df_long["Parameter"] = df_long.apply(
        lambda r: f"{r['Parameter']} ({r['Unit']})" if pd.notna(r["Unit"]) and r["Unit"] != "" else r["Parameter"],
        axis=1,
    )
    df_long["Value"] = df_long["Value"].apply(clean_value_string)
    df_long["Value"] = pd.to_numeric(df_long["Value"], errors="coerce")
    df_long["SampleID"] = sample_id

    # Pivot to wide: one row, Parameter names as columns
    wide = df_long.pivot_table(index="SampleID", columns="Parameter", values="Value", aggfunc='first').reset_index()

    # Ensure all required CBC columns exist
    for col in required_cols:
        if col not in wide.columns:
            wide[col] = np.nan

    # Add / override DateProcessed
    wide["DateProcessed"] = datetime.now().strftime("%Y-%m-%d")

    # Reorder columns: SampleID, DateProcessed, required, then any extras
    main_cols = ["SampleID", "DateProcessed"] + [c for c in required_cols if c in wide.columns]
    other_cols = [c for c in wide.columns if c not in main_cols]
    wide = wide[main_cols + other_cols]

    return wide
