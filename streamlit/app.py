import streamlit as st
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.db.connection import query_data

st.set_page_config(
    page_title="NBA MVP Predictor"
)

st.title('Leaderboard')

query = "SELECT * FROM serving.leaderboard"

df = query_data(query, user='app')
df.drop(columns=['player_id'], inplace=True)

st.dataframe(df, use_container_width=True, hide_index=True)