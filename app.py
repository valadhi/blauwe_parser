# app.py
import streamlit as st

from auth_config import get_authenticator

st.set_page_config(page_title="Report Parser", layout="wide")

# Create / restore authenticator
authenticator, config = get_authenticator()

# ---- LOGIN WIDGET ----
try:
    authenticator.login()  # location defaults to 'main'
except Exception as e:
    st.error(e)

auth_status = st.session_state.get("authentication_status", None)

if auth_status:
    # Logged in
    authenticator.logout(location="sidebar", key="logout_button")
    username = st.session_state.get("username")
    name = st.session_state.get("name", username)

    st.title("Upload → Process → Visualize")
    st.write(f"Logged in as **{name}** (`{username}`)")

    # This username will be our `user_id` in samples.db
    user_id = username

    mode = st.radio(
        "Choose input format:",
        [
            "Port of Rotterdam (PDF + Excel)",
            "Municipality Leiden (Type1 + Type2 PDFs)",
        ],
    )

    if mode == "Port of Rotterdam (PDF + Excel)":
        from pipelines.pipeline_typeA import run_pipeline
        run_pipeline(user_id)

    elif mode == "Municipality Leiden (Type1 + Type2 PDFs)":
        from pipelines.pipeline_typeB import run_pipeline
        run_pipeline(user_id)

elif auth_status is False:
    st.error("Username/password is incorrect")
else:
    st.warning("Please enter your username and password")
