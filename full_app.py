import streamlit as st
import pandas as pd
from utils import *
from dotenv import load_dotenv
import os
import duckdb
# Load environment variables from .env file
load_dotenv()
ANYTHINGLLM_API_KEY = os.getenv('ANYTHINGLLM_API_KEY')
ANYTHINGLLM_API_URL = os.getenv('ANYTHINGLLM_API_URL')
TRANSLATE_SQL_WORKSPACE = os.getenv('TRANSLATE_SQL_WORKSPACE')

# Set page configuration
st.set_page_config(layout="wide")

# Initialize session state for chat messages and df if they don't exist
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'df' not in st.session_state:
    st.session_state.df = None  # Initialize the DataFrame in session state

# Create two columns: one for chat (30% width) and one for DataFrame (70% width)
chat_col, df_col = st.columns([3, 7])



# Chat interface
with chat_col:
    st.header("Chat Interface")

    # Display chat messages
    for message in st.session_state.messages:
        st.text(message)
    def submit():
        #st.session_state.chat_input = ""
        pass
    # Chat input
    user_input = st.text_input("Type your message here:")
    if st.button("Send"):
        if user_input:
            # Append user message to chat
            st.session_state.messages.append(f"You: {user_input}")
            
            # Prepare data for the API if df is available
            if st.session_state.df is not None:
                headers = {
                    'Authorization': f'Bearer {ANYTHINGLLM_API_KEY}',
                    'Content-Type': 'application/json'
                }
                df = st.session_state.df
                print(df.columns)
                schema_str = ', '.join([f'{col}: {dtype}' for col, dtype in zip(df.columns, df.dtypes)])
                
                data = {
                    "message": f"""with this schema: {schema_str}, translate the query below into SQL.
                    For the FROM statement, always do 'FROM df'
                    Ensure that the output is SQL statement only, with no additional text.

                    Query: {user_input}""",
                    "mode": "query"
                }

                # Send the API request
                api_response = post_to_api(f"{ANYTHINGLLM_API_URL}/{TRANSLATE_SQL_WORKSPACE}/chat", data, headers)
                
                # Handle the response from the API
                if api_response['status'] == 200:
                    st.session_state.messages.append(f"Response: {api_response['data'].get('textResponse', 'No reply found')}")
                    sql_query = clean_sql(api_response['data'].get('textResponse'))
                    #apply sql into the df
                    st.session_state.df = duckdb.sql(sql_query).df()
                else:
                    st.session_state.messages.append(f"Response: {api_response['message']}")
            else:
                st.session_state.messages.append("Response: No DataFrame available to generate SQL schema.")
            # st.session_state.chat_input = ""

            st.rerun()
         

# DataFrame display
with df_col:
    st.header("DataFrame Display")

    # Create a smaller container for the file uploader
    with st.container():
        st.subheader("Upload a CSV file")
        uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"], label_visibility="collapsed")

    if uploaded_file is not None:
        # Read the uploaded CSV file into a DataFrame and store it in session state
        st.session_state.df = pd.read_csv(uploaded_file)

        # Display the uploaded DataFrame
        st.dataframe(st.session_state.df, use_container_width=True)
    elif st.session_state.df is not None:
        # If a DataFrame has already been uploaded, display it
        st.dataframe(st.session_state.df, use_container_width=True)
