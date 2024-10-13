import streamlit as st
import pandas as pd
from utils import *
from dotenv import load_dotenv
import os
import duckdb
import logging

# Configure logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler('app.log')  # Log will be written to 'app.log'
file_handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handlers to the logger
log.addHandler(file_handler)


# Load environment variables from .env file
load_dotenv()
ANYTHINGLLM_API_KEY = os.getenv('ANYTHINGLLM_API_KEY')
ANYTHINGLLM_API_URL = os.getenv('ANYTHINGLLM_API_URL')
TRANSLATE_SQL_WORKSPACE = os.getenv('TRANSLATE_SQL_WORKSPACE')

# Set page configuration
st.set_page_config(layout="wide")

# Chat Window init and Style
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'df' not in st.session_state:
    st.session_state.df = None  # Initialize the DataFrame in session state

chat_window_css = """
    <style>
    .chat-window {
        height: 50vh;
        overflow-y: auto;
        padding: 10px;
        margin-y: 10px
        border: 1px solid #ccc;
        background-color: gray;
    }
    .chat-message {
        margin-bottom: 10px;
    }
    .chat-button, sql-button {
        position: absolute;
        position: 0;
    }
    </style>
"""

# Query init
if 'response' not in st.session_state:
    st.session_state.response = None
    
# Traceback stack init
if 'df_stack' not in st.session_state:
    st.session_state.df_stack = []

# Create two columns: one for chat (40% width) and one for DataFrame (60% width)
chat_col, df_col = st.columns([4, 6])

# Chat interface
with chat_col:
    st.header("Chat Interface")

    # CHAT WINDOW
    st.markdown(chat_window_css, unsafe_allow_html=True)
    
    # Create a scrollable chat window
    with st.container(height=350, border=True):
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    
    # Chat input
    with st.container():
        user_input_col, button_col = st.columns([8,2])
        
        with user_input_col:
            user_input = st.text_input("Query:")
        with button_col:
            st.markdown("<div class='chat-button'>", unsafe_allow_html=True)
            
            if st.button("Send"):
                if user_input:
                    # Append user message to chat
                    st.session_state.messages.append({"role":"user", "content":user_input})
                    
                    # Prepare data for the API if df is available
                    if st.session_state.df is not None:
                        headers = {
                            'Authorization': f'Bearer {ANYTHINGLLM_API_KEY}',
                            'Content-Type': 'application/json'
                        }
                        df = st.session_state.df
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
                        st.session_state.response = api_response
                        
                        # Handle the response from the API
                        if api_response['status'] == 200:
                            st.session_state.messages.append({"role":"ai", "content":api_response['data'].get('textResponse', 'No reply found')})
                        else:
                            st.session_state.messages.append({"role":"ai", "content":{api_response['message']}})
                    else:
                        st.session_state.messages.append({"role":"ai", "content": "No DataFrame available to generate SQL schema."})
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        
    # Query input
    with st.container():
        query_input_col, query_button_col = st.columns([8,2])
        
        with query_input_col:
            # Check if the response is available before accessing it
            if st.session_state.response is not None:
                query = st.session_state.response['data'].get('textResponse', 'No reply found')
                if query == 'No reply found':
                    query = None
            else:
                query = None  # Default to empty if response is None

            query_input = st.text_input("SQL:", value=query)
        with query_button_col:
            st.markdown("<div class='sql-button'>", unsafe_allow_html=True)
            
            if st.button("Execute"):
                if query_input:
                    sql_query = clean_sql(query_input)
                    #apply sql into the df
                    df = st.session_state.df
                    updated_df = duckdb.sql(sql_query).df()
                    st.session_state.df_stack.append(updated_df)    # Update df traceback stack
                    st.session_state.df = updated_df                # Update current df
                    st.rerun()
            
            st.markdown("</div>", unsafe_allow_html=True)

# DataFrame display
with df_col:
    uploaded_files = st.file_uploader("Choose CSV files", type=["csv"], accept_multiple_files=True, label_visibility="collapsed")
    if st.session_state.df is not None:
        # If a DataFrame has already been uploaded, display it
        st.dataframe(st.session_state.df, use_container_width=True)
    elif uploaded_files:
        # Initialize an empty list to hold the DataFrames and a variable for the schema
        dataframes = []
        schema = None  # Variable to store the schema of the first DataFrame

        for uploaded_file in uploaded_files:
            df = pd.read_csv(uploaded_file)
            
            # Check if schema is consistent
            if schema is None:
                schema = set(df.columns)  # Set schema for the first DataFrame
            elif schema != set(df.columns):
                st.warning("Failed to concatenate the DataFrames: One or more CSV files have a different schema.")
                st.session_state.df = None  # Reset the DataFrame in session state
                break  # Exit the loop if schemas do not match
            
            dataframes.append(df)

        st.session_state.origin_df = pd.concat(dataframes, ignore_index=True)   # save original df
        st.session_state.df_stack.append(st.session_state.origin_df)            # init df traceback stack
        st.session_state.df = st.session_state.origin_df                        # init current df
        # Display the concatenated DataFrame
        st.dataframe(st.session_state.df, use_container_width=True)
    
    if st.button("Reset"):
        st.session_state.df = st.session_state.origin_df
        st.rerun()
    
    if st.button("undo"):
        if len(st.session_state.df_stack) > 1:
            log.info(f"Undoing last operation. Stack length: {len(st.session_state.df_stack)}")
            popped_df = st.session_state.df_stack.pop()
            st.session_state.df = st.session_state.df_stack[-1]
            st.rerun()