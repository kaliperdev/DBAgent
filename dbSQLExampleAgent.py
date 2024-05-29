import pandas as pd
import openai
import streamlit as st
import os

# Function to set API key
def set_api_key():
    if 'api_key' in st.session_state:
        openai.api_key = st.session_state.api_key
    else:
        st.session_state.api_key = ""

# Initialize API key from environment or session state
if "api_key" in st.session_state and st.session_state.api_key:
    openai.api_key = st.session_state.api_key
else:
    openai.api_key = os.getenv("api_key")

# Streamlit UI to input API key if not already set
if not openai.api_key:
    st.text_input("Enter your OpenAI API key:", key="api_key", type="password", on_change=set_api_key)

if openai.api_key:
    # Load schema CSV
    schema_file_path = 'Schema.csv'
    schema_df = pd.read_csv(schema_file_path)

    # Load examples CSV
    examples_file_path = 'Examples.csv'
    examples_df = pd.read_csv(examples_file_path)

    # Prepare schema information
    schema_info = ""
    for _, row in schema_df.iterrows():
        schema_info += f"Table: {row['Table Name']}\nColumn: {row['Column Name']}\nDescription: {row['Column Description']}\n\n"

    # Prepare examples
    examples = ""
    for _, row in examples_df.iterrows():
        examples += f"Question: {row['Question']}\nQuery: {row['Query']}\n\n"

    # Initialize conversation history in session state if not already initialized
    if 'messages' not in st.session_state:
        st.session_state['messages'] = []

    def generate_sql(conversation):
        prompt = f"""
        You are an expert SQL query writer. Given the following schema and examples, generate a SQL query for the given question. Be mindful of the following: 1. The query should only contain tables and columns as per the schema. For help in generating the query, refer to the examples.

        Schema:
        {schema_info}

        Examples:
        {examples}

        Conversation:
        {conversation}
        """
        response = openai.ChatCompletion.create(
            model="gpt-4",  # Use the GPT-4 model for chat completions
            messages=[
                {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.5,
            n=1,  # Only generate one response
            stop=None
        )
        return response.choices[0]['message']['content'].strip()

    # Streamlit interface
    st.title("SQL Query Generator")

    # Display conversation history
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    user_question = st.text_input("Enter your question:")

    if st.button("Send"):
        if user_question:
            sql_query = generate_sql(user_question)
            st.session_state.messages.append({"role": "user", "content": user_question})
            st.session_state.messages.append({"role": "assistant", "content": sql_query})

    # Display chat history
    st.write("### Chat History")
    for message in reversed(st.session_state.messages):
        if message['role'] == 'user':
            st.write(f"**User:** {message['content']}")
        else:
            st.write(f"**Assistant:** {message['content']}")
            st.code(message['content'], language='sql')
else:
    st.warning("Please enter your OpenAI API key to proceed.")
