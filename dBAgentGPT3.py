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

openai.api_key = st.secrets.credentials.api_key
client = OpenAI()

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
    You are an expert SQL query writer. Given the following schema and examples, generate a SQL query for the given question. Be mindful of the following: 1. The query should only contain tables and columns combinations as per the schema. For help in generating the query, refer to the examples. If there is no schema passed. Display message that no schema available for this query.

    Schema:
    {schema_info}

    Examples:
    {examples}

    Conversation:
    {conversation}
    """

    full_prompt = [
        {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards. These queries should follow format from examples and Schema file. Query should not be out of schema provided, this is most crucial, especially make sure of schema when you are giving join statements with ON clause, and filters. Dont Assume. Also add 'Generated SQL Query:' term just before sql query to identify, don't add any other identifier, apart from text 'Generated SQL Query:', and don't write anything after the query ends."},
        {"role": "user", "content": prompt}
    ]

    enc = tiktoken.get_encoding("cl100k_base")
    token_count = sum([len(enc.encode(message["content"])) for message in full_prompt])
    st.write(token_count)

    stream = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=full_prompt,
        stream=True,
    )
    
    sql_query = ""
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            sql_query += chunk.choices[0].delta.content
            st.write(chunk.choices[0].delta.content, end="")  # Displaying the stream content in real-time in Streamlit
    return sql_query.strip()

def handle_error(query, error):
    prompt = f"""
    Given the following SQL, and the error from Snowflake, along with user conversation. Resolve this. Also add 'Generated SQL Query:' term just before sql query to identify, don't add any other identifier like 'sql' or '`' in response

    Error:
    {error}

    Code:
    {query}

    Conversation:
    {conversation}
    """
    
    stream = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards. Also add 'Generated SQL Query:' term just before sql query to identify, don't add any other identifier like 'sql' or '`' in response, apart from text 'Generated SQL Query:' and don't write anything after the query ends."},
            {"role": "user", "content": prompt}
        ],
        stream=True,
    )
    
    corrected_sql_query = ""
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            corrected_sql_query += chunk.choices[0].delta.content
            st.write(chunk.choices[0].delta.content, end="")  # Displaying the stream content in real-time in Streamlit
    return corrected_sql_query.strip()

def extract_query_from_message(content):
    if "Generated SQL Query:" in content:
        query_part = content.split("Generated SQL Query:", 1)[1].strip()
        if query_part.startswith("```sql") and query_part.endswith("```"):
            return query_part[6:-3].strip()
        elif query_part.startswith("```") and query_part.endswith("```"):
            return query_part[3:-3].strip()
        return query_part
    return content

def generate_chart_code(dataframe):
    prompt = f"""
    You are an expert in data visualization. Given a pandas DataFrame with the following columns: {', '.join(dataframe.columns)}, generate the best charting code using Plotly. The code should produce an informative and visually appealing chart.

    Data to be plotted:
    {dataframe}
    """

    full_prompt = [
        {"role": "system", "content": "You are an expert in data visualization using Plotly. Brand colour is purple, use majorly white and purple shades. give proper visible dark legends, title, and data axis for white background. Make a 3D looking chart in 2D, that looks professional and super appealing. use valid hex color code as color id in code. Start python code with string '```python' and end with '```'"},
        {"role": "user", "content": prompt}
    ]
    
    stream = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=full_prompt,
        stream=True,
    )
    
    chart_code_response = ""
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            chart_code_response += chunk.choices[0].delta.content
            st.write(chunk.choices[0].delta.content, end="")  # Displaying the stream content in real-time in Streamlit
    return chart_code_response.strip()

def extract_code_from_response(response):
    code_block = re.search(r'```python(.*?)```', response, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()
    return ""

if openai.api_key:
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
    st.warning(f"Please enter your OpenAI API key to proceed. {st.secrets.credentials.sf_password}")