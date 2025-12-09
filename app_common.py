import re
import numpy as np
import pandas as pd
from cbc.cbc_core import rename_map, required_cols
from datetime import datetime

def normalize_id(s: str) -> str:
    """Normalize sample identifiers like 'BW23S1' from noisy text/filenames."""
    if s is None:
        return ""
    s = str(s).upper().strip()
    # Prefer canonical pattern BW<digits>S<digits> if present
    m = re.search(r"(BW\d+S\d+)", s)
    if m:
        return m.group(1)
    # Fallback: strip non-alphanumerics
    return re.sub(r"[^A-Z0-9]+", "", s)

def combine_data(particles_df, sample_df, sample_name):
    # 1) strict match – works for BWxxSyy and for exact ids
    matching_cols = [c for c in particles_df.columns if c.startswith(sample_name)]

    # 2) fallback: MV-prefix match for Leiden Type B
    if not matching_cols:
        m = re.match(r"(MV\d+)", sample_name)
        if m:
            prefix = m.group(1)
            matching_cols = [c for c in particles_df.columns if c.startswith(prefix)]

    if not matching_cols:
        raise ValueError(f"No matching particle column for sample prefix '{sample_name}'")

    if len(matching_cols) > 1:
        print(f"Warning: multiple particle columns for '{sample_name}': {matching_cols}. Using the first one.")

    matched_col = matching_cols[0]

    part = particles_df[["Parameter", matched_col]].rename(columns={matched_col: sample_name})
    combined = pd.concat([part, sample_df], ignore_index=True)
    return combined, sample_name


def reshape_cbc(input_df, sample_name):
    df = input_df.copy()
    df[sample_name] = pd.to_numeric(df[sample_name], errors="coerce")

    df["Parameter"] = df["Parameter"].replace(rename_map)
    df = df.dropna(subset=["Parameter"])

    df = df.groupby("Parameter", as_index=False)[sample_name].mean()

    wide = df.set_index("Parameter").T

    for col in required_cols:
        if col not in wide.columns:
            wide[col] = np.nan

    wide["SampleID"] = sample_name
    wide["DateProcessed"] = datetime.now().strftime("%Y-%m-%d")

    return wide.reset_index(drop=True)
