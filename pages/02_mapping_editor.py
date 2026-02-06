# pages/02_mapping_editor.py
import os
import sys
import sqlite3
import streamlit as st
import pandas as pd

# Make parent folder importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from auth_config import get_authenticator
from db.samples_store import get_conn as get_samples_conn, get_parameter_mappings, update_parameter_mapping

st.set_page_config(page_title="Parameter Mapper", layout="wide")

# --- AUTH CHECK ---
authenticator, config = get_authenticator()
try:
    authenticator.login(location="unrendered")
except Exception:
    pass

if not st.session_state.get("authentication_status"):
    st.warning("Please log in from the main page.")
    st.stop()

user_id = st.session_state.get("username")
st.title("🛠️ Parameter Mapping Editor")

# --- DATABASE CONNECTIONS ---
samples_conn = get_samples_conn()
bagger_db_path = "baggerTool_v7.db"

if not os.path.exists(bagger_db_path):
    st.error(f"Rules database '{bagger_db_path}' not found.")
    st.stop()

rules_conn = sqlite3.connect(bagger_db_path)

# --- 1. SELECT PDF ---
st.sidebar.header("1. Selection")

# Get list of PDFs available for this user
pdf_query = samples_conn.execute(
    "SELECT DISTINCT pdf_id FROM extracted_samples WHERE user_id = ? ORDER BY pdf_id",
    (user_id,)
)
pdf_list = [r[0] for r in pdf_query.fetchall()]

if not pdf_list:
    st.info("No extracted PDFs found.")
    st.stop()

selected_pdf = st.sidebar.selectbox("Select Extracted PDF", pdf_list)

# --- 2. SELECT USAGE TARGET ---
# Get list of Targets from Rules DB
target_query = rules_conn.execute("SELECT TargetID, Name FROM TARGET ORDER BY Name")
targets = {r[1]: r[0] for r in target_query.fetchall()}  # Name -> ID

selected_target_name = st.sidebar.selectbox("Select Usage Scenario (Target)", list(targets.keys()))
selected_target_id = targets[selected_target_name]

# --- 3. DATA FETCHING ---

# A. Get required properties (Eigenschappen) for this Target where Weight > 0
# We join HEEFT, TARGET, and EIGENSCHAP
req_query = """
    SELECT E.Name, H.Weight
    FROM HEEFT H
    JOIN EIGENSCHAP E ON H.EigID = E.EigID
    WHERE H.TargetID = ? AND H.Weight > 0
    ORDER BY E.Name
"""
req_rows = rules_conn.execute(req_query, (selected_target_id,)).fetchall()
required_props = [r[0] for r in req_rows]

# B. Get available parameters from the selected PDF
# We grab unique parameters extracted from the file
param_query = """
    SELECT DISTINCT parameter, unit 
    FROM extracted_samples 
    WHERE user_id = ? AND pdf_id = ?
    ORDER BY parameter, unit
"""
param_rows = samples_conn.execute(param_query, (user_id, selected_pdf)).fetchall()
# Helper function to match load_sample_wide logic exactly
def format_full_id(param, unit):
    # Logic must match db/samples_store.py -> load_sample_wide
    if unit and unit.strip():
        return f"{param} ({unit})"
    return param

# Create the list of options using the full ID
available_params = [format_full_id(r[0], r[1]) for r in param_rows]

# C. Get current saved mappings
current_mappings = get_parameter_mappings(samples_conn)

# --- 4. MAPPING INTERFACE ---

st.subheader(f"Map Parameters for: {selected_target_name}")
st.info(
    "Map the properties required by the calculation (Left) to the parameters extracted from your PDF (Right). "
    "Changes are saved automatically."
)

# Create a container for the mapping rows
mapping_container = st.container()

# Pre-calculate inverse mapping for display: which source param is currently mapped to this target?
# current_mappings is {source: target}. We need {target: source} for the selectbox default.
# Note: One source maps to one target. Multiple sources *could* theoretically map to one target, but usually not.
target_to_source = {v: k for k, v in current_mappings.items()}

with mapping_container:
    # Header
    c1, c2, c3 = st.columns([2, 2, 1])
    c1.markdown("**Required Property (Rules DB)**")
    c2.markdown("**Extracted Parameter (PDF)**")
    c3.markdown("**Current Status**")

    st.divider()

    for prop in required_props:
        col1, col2, col3 = st.columns([2, 2, 1])

        # 1. Required Property Name
        col1.write(f"**{prop}**")

        # 2. Dropdown to select source parameter
        # Determine index: if a mapping exists for this property, find the source param in the available list
        active_source = target_to_source.get(prop)

        # Options: add a "Select..." placeholder and the option to Clear/Reset
        options = ["(Unmapped)"] + available_params

        # Calculate index
        try:
            current_index = options.index(active_source) if active_source in options else 0
        except ValueError:
            current_index = 0

        selected_source = col2.selectbox(
            f"Select map for {prop}",
            options,
            index=current_index,
            key=f"sel_{prop}",
            label_visibility="collapsed"
        )

        # 3. Status/Action
        mapped_val = None
        if selected_source != "(Unmapped)":
            # Check if this source is actually present in the current PDF
            is_present = selected_source in available_params
            if is_present:
                col3.success("Mapped")
            else:
                col3.warning("Mapped (Not in PDF)")

            # Logic to save to DB if changed
            if selected_source != active_source:
                update_parameter_mapping(samples_conn, selected_source, prop)
                st.toast(f"Saved: {selected_source} -> {prop}")
                # We force a rerun to update the 'current_mappings' dictionary for other rows
                # strictly speaking optional, but keeps state clean
                # st.rerun()
        else:
            col3.caption("Missing")
            # If it was previously mapped, remove it
            if active_source is not None:
                update_parameter_mapping(samples_conn, active_source, "RESET")
                st.toast(f"Removed mapping for {prop}")
                # st.rerun()

st.divider()

# --- 5. SHOW UNMAPPED PARAMETERS (Helper) ---
st.markdown("### Unused Extracted Parameters")
st.write("The following parameters were found in the PDF but are not currently mapped to any rule property:")

mapped_sources = set(current_mappings.keys())
unused = [p for p in available_params if p not in mapped_sources]

if unused:
    st.code(", ".join(unused), language="text")
else:
    st.success("All extracted parameters are mapped!")

# Cleanup
samples_conn.close()
rules_conn.close()