import streamlit as st
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.db.connection import query_data

st.set_page_config(
    page_title="NBA MVP Predictor"
)

st.title('Leaderboard')

leaderboard_query = "SELECT * FROM serving.leaderboard"

df_mvp = query_data(leaderboard_query, user='app')
df_mvp.drop(columns=['player_id'], inplace=True)

st.dataframe(df_mvp, use_container_width=True, hide_index=True)

update_query = """
    SELECT end_time
    FROM metadata.update_runs
    WHERE status='success'
    ORDER BY end_time DESC
    LIMIT 1
"""

df_update = query_data(update_query, user='app')
last_update = pd.to_datetime(df_update['end_time'][0]).date()

st.caption(f"Last updated: {last_update}")