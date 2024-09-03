import pandas as pd
import openai
import streamlit as st
import os
import plotly.express as px
import tiktoken
import re
from pyairtable import Table
from google.cloud import bigquery
from plotly.subplots import make_subplots
import plotly.graph_objs as go

# Ensure session state is initialized at the very beginning
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Load sensitive information from Streamlit secrets and environment variables
openai.api_key = st.secrets.credentials.api_key
personal_access_token = st.secrets.credentials.airtable_pat
base_id = 'app4ZQ9jav2XzNIv9'  # Airtable base ID
schema_table_id = 'tbl87TPsWhnxSnWw8'  # Airtable table ID for BQNewSchemaColumn
bigquery_credentials_path = "rudderevents-d409acb5f033.json"  # Path to your BigQuery credentials JSON file
bigquery_project_id = "rudderevents"  # Your Google Cloud project ID

# Initialize BigQuery client
bigquery_client = bigquery.Client.from_service_account_json(bigquery_credentials_path)

def load_data(personal_access_token, base_id, schema_table_id):
    """Load active schema data from Airtable."""
    schema_table = Table(personal_access_token, base_id, schema_table_id)
    schema_records = schema_table.all()
    schema_df = pd.DataFrame([record['fields'] for record in schema_records])
    return schema_df

def prepare_schema_info(schema_df):
    """Prepare schema information from DataFrame with only active columns."""
    active_schema_df = schema_df[schema_df['Status'].str.lower() == 'active']
    schema_info = ""

    for _, row in active_schema_df.iterrows():
        schema_info += (
            f"Schema: {row['Schema Name']}\n"
            f"Table: {row['Table Name']}\n"
            f"Column: {row['Column Name']}\n\n"
        )

    return schema_info, active_schema_df

def generate_pseudocode(conversation, schema_info, active_schema_df):
    """Generate step-wise pseudocode for SQL generation."""
    prompt = f"""
    You are an expert at generating step-wise instructions for SQL generation. Given the active schema information below, generate human-readable instructions for the given user query in steps. 
    Ensure the pseudocode and sql query are generated, and those makes sense of the schema, table, and column names. If a requested column, table, or schema is missing from the active list, mention this in the pseudocode, don't generate false pseudocode.
    Schema Information:
    {schema_info}
    Conversation:
    {conversation}
    """
    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a Query Expert who generates step-wise instructions for SQL Query generation. Keep it short and accurate. Don't give SQL Query in response."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=2000,
        temperature=0.1
    )
    return response.choices[0].message.content.strip()


# Load schema from Airtable once when the application starts
schema_df = load_data(personal_access_token, base_id, schema_table_id)
schema_info, active_schema_df = prepare_schema_info(schema_df)

st.title("BI Automation")

# Streamlit interface
user_question = st.text_input("Define your Marketing Analytics Requirements:")
if st.button("Send"):
    if user_question:
        conversation = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state.messages])
        pseudocode = generate_pseudocode(conversation + f"\nUser: {user_question}", schema_info, active_schema_df)
        st.session_state.messages.append({"role": "user", "content": user_question})
        st.session_state.messages.append({"role": "assistant", "content": pseudocode})
        
        st.write("### Generated Pseudocode")
        st.write(pseudocode)
        
        # Extract the SQL query from pseudocode if needed
        # Implement further steps as required
    else:
        st.warning("Please enter your question.")

# Display chat history
st.write("### Chat History")
for message in reversed(st.session_state.messages):
    if message['role'] == 'user':
        st.write(f"**User:** {message['content']}")
    else:
        st.write(f"**Assistant:** {message['content']}")
