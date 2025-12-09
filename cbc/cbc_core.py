import os
import re
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
import pdfplumber
import streamlit as st

# ---------------------------
# 1) Parameter rename map (from notebook)
# ---------------------------
rename_map = {
    # Carbon / Chemistry
    "Anorg. koolstof (CaCO3) % (m/m)": "Kalk (CaCO3)",
    "Q Anorganisch koolstof (als C) g/kg": "Kalk (CaCO3)",
    "C-anorganisch": "Kalk (CaCO3)",
    "C-organisch": "Gehalte organische koolstof (TOC)",
    "Organische stof": "Gehalte organische stof",
    "S Organische stof % (m/m)": "Gehalte organische stof",
    "Koolzure kalk": "Kalk (CaCO3)",
    "N-totale bodemvoorraad": "Stikstof totaal (N-Kjeldahl)",
    "S-totale bodemvoorraad": "Zwavel totaal",
    "S-plantbeschikbaar": "Zwavel beschikbaar",
    "P-plantbeschikbaar": "Fosfor beschikbaar",
    "P-bodemvoorraad": "Fosfor totaal (destructie)",
    "K-plantbeschikbaar": "Kalium beschikbaar",
    "K-bodemvoorraad": "Kalium totaal",
    "pH (1:2,5)": "pH-waarde",

    # Texture / particle fractions (common Dutch lab terms → CBC names)
    "Grootste korrel": "Korrelverdeling (M50)",
    "Korrelverdeling M50": "Korrelverdeling (M50)",
    "Korrelverdeling D60/D10": "Korrelverdeling (D60/D10)",
    "Fractie tot 2 um (lutum)": "Lutum (fractie < 2um)",
    "Fractie tot 10 um (leemfractie)": "Leemfractie (fractie < 10um)",
    "Fractie tot 20 um (silt)": "Silt (2um < fractie < 63um)",
    "Fractie tot 63 um": "(20 < fractie < 63um)",
    "Fractie 63um-250um": "(63um < fractie < 250um)",
    "Fractie 250um-2mm": "(250um < fractie < 2mm)",
    "Zand": "Zand (63um < fractie < 2mm)",
    "Leem": "Leem (lutum+silt)",
    "Grof materiaal": "Grof materiaal (fractie)",
    "Bodemvreemd": "Bodemvreemd",

    # Metals (subset used in notebook)
    "S Koper (Cu) mg/kg": "Koper totaal",
    "S Zink (Zn) mg/kg": "Zink totaal",

    # Entries to drop (no direct mapping)
    "Q Gloeirest % (m/m)": None,
    "S Droge stof %": None,
    "S Arseen (As) mg/kg": None,
    "S Barium (Ba) mg/kg": None,
    "S Cadmium (Cd) mg/kg": None,
    "S Chroom (Cr) mg/kg": None,
    "S Kwik (Hg) mg/kg": None,
    "S Lood (Pb) mg/kg": None,
    "S Nikkel (Ni) mg/kg": None,
}

required_cols = [
    "Mineralen delen ten opzichte DS",
    "Zand (63um < fractie < 2mm)",
    "(250um < fractie < 2mm)",
    "(63um < fractie < 250um)",
    "Silt (2um < fractie < 63um)",
    "(20 < fractie < 63um)",
    "Leemfractie (fractie < 10um)",
    "Lutum (fractie < 2um)",
    "Leem (lutum+silt)",
    "Korrelverdeling (D60/D10)",
    "Korrelverdeling (M50)",
    "Gehalte organische koolstof (TOC)",
    "Gehalte organische stof",
    "Kalk (CaCO3)",
    "pH-waarde",
    "Grof materiaal (fractie)",
    "Bodemvreemd",
    "Fosfor totaal (destructie)",
    "Fosfor beschikbaar",
    "Stikstof totaal (N-Kjeldahl)",
    "Ammonium totaal",
    "Stikstof levering",
    "C/N verhouding",
    "Zwavel totaal",
    "Zwavel beschikbaar",
    "Kalium totaal",
    "Kalium beschikbaar",
    "Koper totaal",
    "Zink totaal",
]

# ---------------------------
# 6) CBC rule engine
# ---------------------------
def run_cbc(sample_wide_df: pd.DataFrame, db_conn: sqlite3.Connection):
    """
    Evaluate CBC rules for a single-sample wide dataframe.

    Returns:
      - results_df: 1-row DataFrame with columns [<targets...>, SampleID, DateProcessed]
      - compact_matrix: DataFrame EigName x TargetName with values {-1, 0, 1}
    """

    def get_table(name: str):
        cs = db_conn.execute(f"SELECT * FROM {name}")
        cols = [d[0] for d in cs.description]
        return [dict(zip(cols, row)) for row in cs]

    target = get_table("TARGET")
    eigenschap = get_table("EIGENSCHAP")
    heeft = get_table("HEEFT")

    # --- build sample dict: key = EigID, value = sample value ---
    row = sample_wide_df.iloc[0]
    sample = {}

    for e in eigenschap:
        eig_id = str(e["EigID"])
        name = str(e["Name"])  # expected column name in sample_wide_df
        if name in sample_wide_df.columns:
            try:
                val = float(row[name]) if pd.notna(row[name]) else -1
            except Exception:
                val = -1
        else:
            val = -1  # sentinel for "missing"
        sample[eig_id] = val

    # --- initialise scores and tracking ---
    target_scores = {str(t["TargetID"]): 0.0 for t in target}
    target_max = {str(t["TargetID"]): 0.0 for t in target}
    pass_fail_matrix = []

    # --- evaluate rules (only Weight == 1, as in your original) ---
    for h in heeft:
        if h["Weight"] != 1:
            continue

        tid = str(h["TargetID"])
        eig_id = str(h["EigID"])
        s = sample.get(eig_id, -1)

        # skip missing sentinel (-1) completely
        if s == -1:
            passed = -1
            pass_fail_matrix.append({
                "TargetID": h["TargetID"],
                "EigID": h["EigID"],
                "EigName": next(e["Name"] for e in eigenschap if e["EigID"] == h["EigID"]),
                "SampleValue": s,
                "Passed": passed,
            })
            continue

        min_val = h["Min"]
        max_val = h["Max"]

        # Only count this rule in the denominator if it's actually applicable
        target_max[tid] += h["Weight"]

        if (min_val is not None) and (max_val is not None) and (min_val <= s <= max_val):
            target_scores[tid] += h["Weight"]
            passed = 1
        else:
            passed = 0

        pass_fail_matrix.append({
            "TargetID": h["TargetID"],
            "EigID": h["EigID"],
            "EigName": next(e["Name"] for e in eigenschap if e["EigID"] == h["EigID"]),
            "SampleValue": s,
            "Passed": passed,
        })

    # --- compute suitability scores per target ---
    results = {}
    for t in target:
        tid = str(t["TargetID"])
        denom = target_max[tid]
        results[t["Name"]] = (target_scores[tid] / denom) if denom > 0 else 0.0

    # convert results dict -> 1-row DataFrame and attach IDs
    results_row = results.copy()
    results_row["SampleID"] = sample_wide_df.loc[0, "SampleID"]
    results_row["DateProcessed"] = sample_wide_df.loc[0, "DateProcessed"]
    results_df = pd.DataFrame([results_row])

    # --- build pass/fail matrix as in your original code ---
    pass_fail_df = pd.DataFrame(pass_fail_matrix)
    if pass_fail_df.empty:
        return results_df, pd.DataFrame()

    target_map = {t["TargetID"]: t["Name"] for t in target}
    pass_fail_df["TargetName"] = pass_fail_df["TargetID"].map(target_map)

    compact_matrix = pass_fail_df.pivot_table(
        index="EigName",
        columns="TargetName",
        values="Passed",
        aggfunc="first",
    ).fillna(-1).astype(int)

    return results_df, compact_matrix

