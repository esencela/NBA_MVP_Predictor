import streamlit as st
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.db.connection import query_data
from source.config.settings import CURRENT_SEASON

st.set_page_config(
    page_title="NBA MVP Predictor"
)

leaderboard_query = """
    SELECT 
        RANK() OVER(
            ORDER BY "Predicted_Share" DESC
        ) AS "Rank",
        * 
    FROM serving.leaderboard
"""

df_mvp = query_data(leaderboard_query, user='app')

# Add headshot URLs based on player_id
df_mvp['Headshot'] = df_mvp['player_id'].apply(
    lambda x: f'https://basketball-reference.com/req/202212091/images/headshots/{x}.jpg'
)

df_mvp['Team_Logo'] = df_mvp['Team'].apply(
    lambda x: f'https://cdn.ssref.net/req/202603120/tlogo/bbr/{x}-{CURRENT_SEASON}.png'
)

df_mvp.drop(columns=['player_id', 'Team'], inplace=True)

df_mvp = df_mvp[['Rank', 'Headshot', 'Player', 'Team_Logo', 'MP','PTS', 'AST', 'TRB', 'STL', 'BLK', 'Predicted_Share']]

st.title('NBA MVP Predictor :basketball:')
st.markdown('Machine learning model to predict the NBA MVP based on player stats and team performance.')
st.divider()

leader = df_mvp.iloc[0]

col1, col2 = st.columns([2, 1])

with col1:
    st.metric(
        label=f'Current MVP Favourite:',
        value=leader['Player'],
        delta=f"{leader['Predicted_Share']:.3f} MVP Share"
    )
    
with col2:
    st.image(leader['Headshot'], width=100)



st.subheader('MVP Leaderboard')

st.dataframe(
    df_mvp, 
    column_config={
        'Headshot': st.column_config.ImageColumn(
            '',
            width=50
        ),
        'Team_Logo': st.column_config.ImageColumn(
            'Team',
            width=50
        ),
        'Predicted_Share': st.column_config.NumberColumn(
            'Vote Share',
            format='%.3f'
        )
    },
    use_container_width=True, 
    hide_index=True
)

update_query = """
    SELECT data_freshness
    FROM metadata.data_freshness
    ORDER BY time_updated DESC
    LIMIT 1
"""

df_update = query_data(update_query, user='app')
data_freshness = pd.to_datetime(df_update['data_freshness'][0]).date()

st.caption(f':clock2: Stats last updated: {data_freshness}')