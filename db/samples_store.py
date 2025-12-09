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
    """Create tables if they don't exist yet."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extracted_samples (
            user_id   TEXT NOT NULL,
            pdf_id    TEXT NOT NULL,
            sample_id TEXT NOT NULL,
            parameter TEXT NOT NULL,
            value     REAL,
            PRIMARY KEY (user_id, pdf_id, sample_id, parameter)
        )
        """
    )
    conn.commit()


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

        # Convert to float / NULL where appropriate
        if value is None or (isinstance(value, float) and np.isnan(value)):
            v = None
        else:
            try:
                v = float(value)
            except Exception:
                v = None

        conn.execute(
            """
            INSERT OR REPLACE INTO extracted_samples
                (user_id, pdf_id, sample_id, parameter, value)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, pdf_id, sample_id, param, v),
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
        SELECT parameter, value
        FROM extracted_samples
        WHERE user_id = ? AND pdf_id = ? AND sample_id = ?
        """,
        (user_id, pdf_id, sample_id),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    df_long = pd.DataFrame(rows, columns=["Parameter", "Value"])
    df_long["SampleID"] = sample_id

    # Pivot to wide: one row, Parameter names as columns
    wide = df_long.pivot(index="SampleID", columns="Parameter", values="Value").reset_index()

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
