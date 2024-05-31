import pandas as pd
import openai
import streamlit as st
import os
import snowflake.connector


# Ensure session state is initialized at the very beginning
if 'messages' not in st.session_state:
    st.session_state.messages = []

st.session_state.SF_User = []
st.session_state.SF_Password = []
st.session_state.SF_Account = []

SNOWFLAKE_USER = st.session_state.SF_User
SNOWFLAKE_PASSWORD = st.session_state.SF_Password
SNOWFLAKE_ACCOUNT = st.session_state.SF_Account
SNOWFLAKE_WAREHOUSE = "RUDDER_WAREHOUSE"
SNOWFLAKE_ROLE = "Rudder"

# Function to set API key
def set_api_key():
    if 'api_key' in st.session_state:
        openai.api_key = st.session_state.api_key
    else:
        st.session_state.api_key = ""



def execute_query(query):

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        )
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetch_pandas_all()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return str(e)

def generate_sql(conversation):
    prompt = f"""
    You are an expert SQL query writer. Given the following schema and examples, generate a SQL query for the given question. Be mindful of the following: 1. The query should only contain tables and columns combinations as per the schema. For help in generating the query, refer to the examples.

    Schema:
    {schema_info}

    Examples:
    {examples}

    Conversation:
    {conversation}
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards. Also add 'Generated SQL Query:' term just before sql query to identify, and don't write anything after the query ends."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1000,
        temperature=0.5,
        n=1,
        stop=None
    )
    return response.choices[0]['message']['content'].strip()

def handle_error(query, error):
    prompt = f"""
    Given the following SQL, and the error from Snowflake. Resolve this. Also add 'Generated SQL Query:' term just before sql query to identify, and don't write anything after the query ends.

    Error:
    {error}

    Code:
    {query}

    """
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1000,
        temperature=0.5,
        n=1,
        stop=None
    )
    return response.choices[0]['message']['content'].strip()

def extract_query_from_message(content):
    if "Generated SQL Query:" in content:
        return content.split("Generated SQL Query:", 1)[1].strip()
    return content

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

    st.title("SQL Query Generator")

    # Streamlit interface
    user_question = st.text_input("Enter your question:")

    if st.button("Send"):
        if user_question:
            conversation = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state.messages])
            sql_query = generate_sql(conversation + f"\nUser: {user_question}")
            st.session_state.messages.append({"role": "user", "content": user_question})
            st.session_state.messages.append({"role": "assistant", "content": sql_query})
            actual_sql_query = extract_query_from_message(sql_query)
            
            result = execute_query(actual_sql_query)
            
            if "SQL compilation error" in result:
                st.error(f"SQL compilation error: {result}")
                corrected_sql_query = handle_error(actual_sql_query, result)
                st.session_state.messages.append({"role": "assistant", "content": corrected_sql_query})
                corrected_sql_query_text = extract_query_from_message(corrected_sql_query)
                result = execute_query(corrected_sql_query_text)
                if "SQL compilation error" in result:
                    st.error(f"Error executing corrected query: {result}")
                else:
                    st.write("### Corrected Query Result")
                    st.write(result)
                    st.session_state.messages.append({"role": "assistant", "content": corrected_sql_query_text})
            else:
                st.write("### Query Result")
                st.write(result)

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

