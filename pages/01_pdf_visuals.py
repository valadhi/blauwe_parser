# pages/01_pdf_visuals.py
import os
import sys
import sqlite3

import streamlit as st
import pandas as pd
import numpy as np

# Make parent folder importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from auth_config import get_authenticator
from db.samples_store import get_conn as get_samples_conn, load_sample_wide
from visuals.visuals import show_sample_visuals
from cbc.cbc_core import run_cbc, required_cols


st.set_page_config(page_title="PDF Visuals – Port of Rotterdam", layout="wide")

# Re-create authenticator and "replay" login (unrendered)
authenticator, config = get_authenticator()
try:
    authenticator.login(location="unrendered")
except Exception as e:
    st.error(e)

auth_status = st.session_state.get("authentication_status", None)

if not auth_status:
    if auth_status is False:
        st.error("Username/password is incorrect. Please go back to the main page and log in.")
    else:
        st.warning("You must log in from the main page to access this view.")
    st.stop()
else:
    authenticator.logout(location="sidebar", key="logout_button")

user_id = st.session_state.get("username")

st.title("View existing PDF samples")

# Connect to samples DB and list available pdf_ids for this user
samples_conn = get_samples_conn()

cur = samples_conn.execute(
    """
    SELECT DISTINCT pdf_id
    FROM extracted_samples
    WHERE user_id = ?
    ORDER BY pdf_id
    """,
    (user_id,),
)
pdf_ids = [r[0] for r in cur.fetchall()]

if not pdf_ids:
    st.info("No stored samples found yet for this user.")
    samples_conn.close()
else:
    selected_pdf = st.selectbox("Select a PDF (certificate)", pdf_ids)

    cur = samples_conn.execute(
        """
        SELECT DISTINCT sample_id
        FROM extracted_samples
        WHERE user_id = ? AND pdf_id = ?
        ORDER BY sample_id
        """,
        (user_id, selected_pdf),
    )
    sample_ids = [r[0] for r in cur.fetchall()]

    if not sample_ids:
        st.warning("No samples found for this PDF.")
        samples_conn.close()
    else:
        selected_samples = st.multiselect(
            "Select samples to visualize",
            sample_ids,
            default=sample_ids,
        )

        if not selected_samples:
            st.info("Choose at least one sample to render visuals.")
            samples_conn.close()
            st.stop()

        # Open rules DB for CBC evaluation
        db_path = "baggerTool_v7.db"
        if not os.path.exists(db_path):
            st.error("Local rules database 'baggerTool_v7.db' not found.")
            samples_conn.close()
            st.stop()
        db_conn = sqlite3.connect(db_path)

        result_rows = []
        matrices = {}

        for sample_name in selected_samples:
            wide = load_sample_wide(samples_conn, user_id, selected_pdf, sample_name, required_cols)
            if wide is None or wide.empty:
                st.warning(f"No wide data reconstructed for sample {sample_name}, skipping.")
                continue

            res_df, pf_df = run_cbc(wide, db_conn)
            result_rows.append(res_df)
            matrices[sample_name] = pf_df

        db_conn.close()
        samples_conn.close()

        if not result_rows:
            st.warning("No CBC results could be computed for this PDF.")
        else:
            result = pd.concat(result_rows, ignore_index=True)
            show_sample_visuals(result, matrices)
