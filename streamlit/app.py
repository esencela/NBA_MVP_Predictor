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
    SELECT data_freshness
    FROM metadata.data_freshness
    ORDER BY time_updated DESC
    LIMIT 1
"""

df_update = query_data(update_query, user='app')
data_freshness = pd.to_datetime(df_update['data_freshness'][0]).date()

st.caption(f"Stats last updated: {data_freshness}")