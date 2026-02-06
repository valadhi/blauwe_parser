# pages/02_mapping_editor.py
import os
import sys
import sqlite3
import streamlit as st

# Make parent folder importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from auth_config import get_authenticator
# [FIX 1] Clean imports: Import the new specific mapping getters
from db.samples_store import (
    get_conn as get_samples_conn,
    update_parameter_mapping,
    get_global_mappings,
    get_local_mappings
)

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
param_query = """
    SELECT DISTINCT parameter, unit 
    FROM extracted_samples 
    WHERE user_id = ? AND pdf_id = ?
    ORDER BY parameter, unit
"""
param_rows = samples_conn.execute(param_query, (user_id, selected_pdf)).fetchall()


# Helper function to match load_sample_wide logic exactly
def format_full_id(param, unit):
    p = str(param).strip()
    u = str(unit).strip() if unit else ""
    if u: return f"{p} ({u})"
    return p


# Create the list of options using the full ID
available_params = [format_full_id(r[0], r[1]) for r in param_rows]

# C. Get current saved mappings (Hybrid Logic)
# [FIX 2] Fetch both maps and merge them. Local overwrites Global.
global_map = get_global_mappings(samples_conn)
local_map = get_local_mappings(samples_conn, user_id, selected_pdf)

combined_map = {**global_map, **local_map}

# --- 4. MAPPING INTERFACE ---

st.subheader(f"Map Parameters for: {selected_target_name}")
st.info(
    "Map the properties required by the calculation (Left) to the parameters extracted from your PDF (Right). "
    "Changes are saved automatically."
)

# Create a container for the mapping rows
mapping_container = st.container()

# [FIX 3] Use combined_map to determine what is currently active
target_to_source = {v: k for k, v in combined_map.items()}

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
        active_source = target_to_source.get(prop)

        # Options: add a "Select..." placeholder
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
        if selected_source != "(Unmapped)":
            is_present = selected_source in available_params

            # [FIX 4] Check specific logic: Is this defined in the Local Map?
            # We look if the property exists in local_map values AND if the key matches selected source
            is_local = prop in local_map.values() and local_map.get(selected_source) == prop

            if is_present:
                if is_local:
                    col3.info("Mapped (Manual)")  # Blue badge for overrides
                else:
                    col3.success("Mapped (Global)")  # Green badge for defaults
            else:
                col3.warning("Mapped (Not in PDF)")

            # Logic to save to DB if changed
            if selected_source != active_source:
                update_parameter_mapping(samples_conn, user_id, selected_pdf, selected_source, prop)
                st.toast(f"Saved: {selected_source} -> {prop}")
                # Optional: st.rerun() to refresh state immediately
        else:
            col3.caption("Missing")

            # If it was previously mapped (active_source was not None), we need to reset it.
            # "Resetting" means removing the LOCAL override.
            if active_source is not None:
                update_parameter_mapping(samples_conn, user_id, selected_pdf, active_source, "RESET")
                st.toast(f"Removed mapping for {prop}")
                # Optional: st.rerun()

st.divider()

# --- 5. SHOW UNMAPPED PARAMETERS (Helper) ---
st.markdown("### Unused Extracted Parameters")
st.write("The following parameters were found in the PDF but are not currently mapped to any rule property:")

# [FIX 5] Use combined_map to calculate unused params
mapped_sources = set(combined_map.keys())
unused = [p for p in available_params if p not in mapped_sources]

if unused:
    st.code(", ".join(unused), language="text")
else:
    st.success("All extracted parameters are mapped!")

# Cleanup
samples_conn.close()
rules_conn.close()