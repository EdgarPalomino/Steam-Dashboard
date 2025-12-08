import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from pathlib import Path
from openai import OpenAI

st.title("Steam Dashboard")

# ---- Load RSA key ----
# key_path = Path(r"C:\Users\jakek\Documents\Data_Management\rsa_key.p8")
with open(key_path, "rb") as key_file:
    private_key_obj = serialization.load_pem_private_key(
        key_file.read(),
        password=None
    )

private_key_bytes = private_key_obj.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# ---- Connect to Snowflake ----
conn = snowflake.connector.connect(
    user="CHEETAH",
    account="NMB12256",
    private_key=private_key_bytes,
    warehouse="CHEETAH_WH",
    database="STEAM_ANALYTICS",
    schema="RAW"
)

def run_query(query: str):
    cur = conn.cursor()
    try:
        cur.execute(query)
        df = cur.fetch_pandas_all()
        return df
    finally:
        cur.close()


OpenAI_API_KEY = "Dummy Key for Testing Only"
client = OpenAI(api_key=OpenAI_API_KEY)
# -------------------------------
# Queries (pull first 5 rows)
# -------------------------------
# steam_id_value = 76561198307954663 # Example Steam ID
steam_id_value = 76561198202353058
player_name = 'NinjaKoala101' # Example Player Name

# df1 = run_query(f"""
#     SELECT NAME, APPID, PLAYTIME_FOREVER
#     FROM GAMES
#     WHERE STEAMID = {steam_id_value}
#     ORDER BY PLAYTIME_FOREVER DESC
#     LIMIT 10;
# """)

df1 = run_query(f"""
    SELECT NAME, APPID, PLAYTIME_FOREVER
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY NAME 
                ORDER BY PLAYTIME_FOREVER DESC
            ) AS rn
        FROM GAMES
        WHERE STEAMID = {steam_id_value}
    )
    WHERE rn = 1
    ORDER BY PLAYTIME_FOREVER DESC
    LIMIT 5;
""")


df2 = run_query(f"""
    SELECT PERSONANAME, STEAMID, PROFILESTATE, REALNAME
    FROM PLAYERS
    WHERE PERSONANAME = '{player_name}'
    LIMIT 5;
""")

df3 = run_query("SELECT * FROM GAMES LIMIT 5;")
df4 = run_query("SELECT * FROM PLAYERS LIMIT 5;")

# -------------------------------
# Layout
# -------------------------------

# ---- First Row (two tables) ----
col1, col2 = st.columns(2)

with col1:
    st.subheader("Steam Leaderboard")
    st.dataframe(df1, use_container_width=True)

with col2:
    st.subheader("User Summary")
    st.dataframe(df2, use_container_width=True)

# # ---- Center Button ----
# left, mid, right = st.columns([2,1,2])
# with mid:
#     button_clicked = st.button("Recomend Me a Game", use_container_width=True)

# if button_clicked:
#     with st.spinner("Analyzing game history..."):
#         # Prepare game list for OpenAI
#         games_list = df1.to_dict(orient="records")

#         prompt_text = f"""
#         Here is the user's recent most-played games (top 5 unique titles):

#         {games_list}

#         Recommend ONE new Steam game that the user has not played yet.
#         Only output the game's name. No explanation, no extra text.
#         """

#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[
#                 {"role": "system", "content": "You recommend Steam games based on user gameplay."},
#                 {"role": "user", "content": prompt_text}
#             ],
#             max_tokens=20,
#             temperature=0.7
#         )

#         # Extract recommendation
#         recommended_game = response.choices[0].message["content"].strip()

#         st.success(f"Recommended Game: **{recommended_game}**")

# ---- Center Button ----
left, mid, right = st.columns([2,1,2])
with mid:
    button_clicked = st.button("Recommend Me a Game", use_container_width=True)

if button_clicked:
    with st.spinner("Analyzing game history..."):
        try:
            # Prepare game list for OpenAI
            games_list = df1.to_dict(orient="records")

            prompt_text = f"""
            Here is the user's recent most-played games (top 5 unique titles):

            {games_list}

            Recommend ONE new Steam game that the user has not played yet.
            Only output the game's name. No explanation, no extra text.
            """

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You recommend Steam games based on user gameplay."},
                    {"role": "user", "content": prompt_text}
                ],
                max_tokens=20,
                temperature=0.7
            )

            # Extract recommendation (updated for v1 API)
            recommended_game = response.choices[0].message.content.strip()
            st.success(f"Recommended Game: **{recommended_game}**")

        except Exception as e:
            st.error("OpenAI API Error:")
            st.code(str(e))
            

# ---- Second Row (two tables) ----
col3, col4 = st.columns(2)

with col3:
    st.subheader("Popluar Games ALl-Time")
    st.dataframe(df3, use_container_width=True)

with col4:
    st.subheader("Game Sentiment Analysis")
    st.dataframe(df4, use_container_width=True)
