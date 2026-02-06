# app.py
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import streamlit as st

from auth_config import get_authenticator
from db.samples_store import get_conn as get_samples_conn, save_extraction_results
from smart_parser_two_pass import process_generic_report


st.set_page_config(page_title="Soil Report Intelligence", layout="wide", page_icon="🌱")

# # Create / restore authenticator
# authenticator, config = get_authenticator()
#
# # ---- LOGIN WIDGET ----
# try:
#     authenticator.login()  # location defaults to 'main'
# except Exception as e:
#     st.error(e)
#

if "authentication_status" not in st.session_state:
    st.session_state["authentication_status"] = True
    st.session_state["username"] = "default_user"
    st.session_state["name"] = "Default User"

# Retrieve User Info
name = st.session_state.get("name", "User")
user_id = st.session_state.get("username", "default_user")

# --- SECRETS MANAGEMENT ---
# Try to get key from Streamlit Secrets, fallback to Environment Variable
api_key = os.getenv("GEMINI_KEY") or st.secrets.get("GEMINI_KEY")
if not api_key:
    st.error("🚨 GEMINI_KEY is missing! Please add it to .streamlit/secrets.toml or Streamlit Cloud Secrets.")
    st.stop()

auth_status = st.session_state.get("authentication_status", None)
if auth_status:
    # Logged in
    # authenticator.logout(location="sidebar", key="logout_button")
    username = st.session_state.get("username")
    name = st.session_state.get("name", username)

    st.title("🌱 Soil Report Intelligence")
    st.markdown("### Intelligent Analysis & Compliance Reporting")

    # This username will be our `user_id` in samples.db
    user_id = username

    col1, col2 = st.columns(2)
    col1.info(f"👤 **User:** {name}")
    # col2.info(f"📂 **Project:** Port of Rotterdam")

    st.divider()

    st.subheader("📄 Import New Report")
    uploaded_file = st.file_uploader("Choose a PDF", type=["pdf"])

    if uploaded_file is not None:
        pdf_id = uploaded_file.name
        pdf_stem = Path(pdf_id).stem

        pdf_bytes = uploaded_file.getvalue()

        cache_dir = Path("extractions")
        cache_dir.mkdir(exist_ok=True)

        # Prefer a cached extraction by original name and fall back to any legacy
        # location in the current working directory. All parser outputs are stored
        # using the original file name to avoid temporary hashes.
        cache_candidates = [
            cache_dir / f"{pdf_stem}_extracted.csv",
            Path(f"{pdf_stem}_extracted.csv"),
        ]

        extracted_path = next((path for path in cache_candidates if path.exists()), None)

        if extracted_path is not None:
            st.info(f"Found existing extraction for '{pdf_id}'. Using cached results from {extracted_path.name}.")
            try:
                extracted_df = pd.read_csv(extracted_path)
            except Exception as e:
                st.error(f"Failed to load cached extraction: {e}")
                extracted_df = None
        else:
            # Persist the uploaded PDF under its original name to keep parser outputs
            # aligned and avoid temporary naming.
            pdf_cache_path = cache_dir / pdf_id
            with open(pdf_cache_path, "wb") as tmp_pdf:
                tmp_pdf.write(pdf_bytes)

            with st.spinner("Running smart parser (two-pass)..."):
                try:
                    # Store all parser outputs (debug + extracted) under cache_dir using the
                    # original file name as the base.
                    extracted_df = process_generic_report(
                        str(pdf_cache_path),
                        output_base_name=str(cache_dir / pdf_stem),
                        api_key=api_key
                    )
                except Exception as e:
                    st.error(f"Parsing failed: {e}")
                    extracted_df = None

        if extracted_df is None or extracted_df.empty:
            st.warning("No samples were extracted from this upload.")
        else:
            samples_conn = get_samples_conn()
            save_extraction_results(samples_conn, user_id, pdf_id, extracted_df)
            samples_conn.close()

            sample_names = sorted(extracted_df["sample_id"].unique())
            parameters = sorted(extracted_df["parameter"].unique())

            st.success(f"Stored {len(sample_names)} samples from '{pdf_id}' for {user_id}.")
            st.write("**Samples detected:**", ", ".join(sample_names))
            st.write("**Parameters found:**", ", ".join(parameters))

elif auth_status is False:
    st.error("Username/password is incorrect")
else:
    st.warning("Please enter your username and password")
