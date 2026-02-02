import streamlit as st
import sys
import os

# Add the project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.db.connection import query_data

st.set_page_config(
    page_title="NBA MVP Predictor"
)

st.title('Leaderboard')

query = "SELECT * FROM serving.leaderboard"

df = query_data(query)

st.dataframe(df)