import pandas as pd
import openai
import streamlit as st
import os
import snowflake.connector
import plotly.express as px
import tiktoken
import re
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from openai import OpenAI

# Ensure session state is initialized at the very beginning
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Check and prompt for Snowflake credentials
SNOWFLAKE_PASSWORD = st.secrets.credentials.sf_password

SNOWFLAKE_USER = "Rahul"
SNOWFLAKE_ACCOUNT = "zt30947.us-east-2.aws"
SNOWFLAKE_WAREHOUSE = "RUDDER_WAREHOUSE"
SNOWFLAKE_DATABASE = "RUDDER_EVENTS"
SNOWFLAKE_ROLE = "Rudder"

# Access the API key from Streamlit secrets and initialize the client
api_key = st.secrets.credentials.api_key
client = OpenAI(api_key=api_key)

def execute_query(query):
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
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
    Generate an SQL query based on the given conversation, schema, and examples. Follow the schema strictly and use the format from the examples provided.

    Schema:
    {schema_info}

    Examples:
    {examples}

    Conversation:
    {conversation}

    Generated SQL Query:
    """

    full_prompt = [
        {"role": "system", "content": "You are an expert SQL query writer for Snowflake databases. Use the provided schema and examples to generate accurate SQL queries. Make sure to use the exact table and column names from the schema."},
        {"role": "user", "content": prompt}
    ]

    enc = tiktoken.get_encoding("cl100k_base")
    token_count = sum([len(enc.encode(message["content"])) for message in full_prompt])
    st.write(f"Token count: {token_count}")

    try:
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=full_prompt,
            stream=True,
        )

        sql_query = ""
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                sql_query += chunk.choices[0].delta.content
                st.write(chunk.choices[0].delta.content)  # Displaying the stream content in real-time in Streamlit
        return sql_query.strip()
    except Exception as e:
        st.error(f"Error generating SQL: {e}")
        return ""

def extract_query_from_message(content):
    # Extract the SQL query from the message content
    match = re.search(r'```sql(.*?)```', content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return content

def handle_error(query, error):
    prompt = f"""
    Resolve the following SQL error for the given query based on the provided schema and conversation.

    Error:
    {error}

    Code:
    {query}

    Conversation:
    {conversation}

    Corrected SQL Query:
    """
    
    try:
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert SQL query writer for Snowflake databases. Resolve SQL errors using the provided schema and conversation context. Include 'Corrected SQL Query:' before your query."},
                {"role": "user", "content": prompt}
            ],
            stream=True,
        )
        
        corrected_sql_query = ""
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                corrected_sql_query += chunk.choices[0].delta.content
                st.write(chunk.choices[0].delta.content)  # Displaying the stream content in real-time in Streamlit
        return corrected_sql_query.strip()
    except Exception as e:
        st.error(f"Error correcting SQL: {e}")
        return ""

def generate_chart_code(dataframe):
    prompt = f"""
    Generate Plotly chart code for the given pandas DataFrame with columns: {', '.join(dataframe.columns)}. The chart should be informative and visually appealing.

    Data to be plotted:
    {dataframe.to_string(index=False)}

    Chart Code:
    """

    full_prompt = [
        {"role": "system", "content": "You are an expert in data visualization using Plotly. Use the given DataFrame to generate professional and appealing chart code. The DataFrame will be provided as 'df'."},
        {"role": "user", "content": prompt}
    ]
    
    try:
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=full_prompt,
            stream=True,
        )
        
        chart_code_response = ""
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                chart_code_response += chunk.choices[0].delta.content
                st.write(chunk.choices[0].delta.content)  # Displaying the stream content in real-time in Streamlit
        return chart_code_response.strip()
    except Exception as e:
        st.error(f"Error generating chart code: {e}")
        return ""

def extract_code_from_response(response):
    code_block = re.search(r'```python(.*?)```', response, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()
    return ""

if api_key:
    schema_file_path = 'Schema.csv'
    schema_df = pd.read_csv(schema_file_path)

    examples_file_path = 'Examples.csv'
    examples_df = pd.read_csv(examples_file_path)

    schema_info = ""
    for _, row in schema_df.iterrows():
        schema_info += f"Table: {row['Table Name']}\nColumn: {row['Column Name']}\nDescription: {row['Column Description']}\n\n"

    examples = ""
    for _, row in examples_df.iterrows():
        examples += f"Question: {row['Question']}\nQuery: {row['Query']}\n\n"

    st.title("BI Automation")

    user_question = st.text_input("Define your Marketing Analytics Requirements:")

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
                st.session_state.messages.append({"role": "assistant", "content": result})

                chart_code_response = generate_chart_code(result)
                st.write("### Chart Code Response")
                chart_code = extract_code_from_response(chart_code_response)
                try:
                    local_scope = {}
                    exec(chart_code, {'pd': pd, 'px': px, 'go': go, 'make_subplots': make_subplots, 'df': result}, local_scope)
                    fig = local_scope.get('fig')
                    if fig:
                        st.plotly_chart(fig)
                    else:
                        st.error("No figure found in the generated code.")
                except Exception as e:
                    st.error(f"Error executing chart code: {e}")

    st.write("### Chat History")
    for message in reversed(st.session_state.messages):
        if message['role'] == 'user':
            st.write(f"**User:** {message['content']}")
        else:
            try:
                fig = px.line(result)
                st.plotly_chart(fig)
            except:
                print("Something is Suspicious")
            st.code(message['content'], language='sql')
            st.write(result)
else:
    st.warning("Please enter your OpenAI API key to proceed.")
