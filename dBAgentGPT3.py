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
    Generate an SQL query based on the given conversation, schema, and examples. Follow the schema strictly and use the logic and filters from the examples provided as a information base to the data, and refer it. Also use it to figure out which column to use on what queries, and those columns are present in table schema. Use identifiers in query very carefully.

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
            model="ft:gpt-3.5-turbo-0125:cubestack-solutions::9ZdYPGXy",
            messages=full_prompt,
            stream=True,
        )

        sql_query = ""
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                sql_query += chunk.choices[0].delta.content
                #st.write(chunk.choices[0].delta.content)  # Displaying the stream content in real-time in Streamlit
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
            model="ft:gpt-3.5-turbo-0125:cubestack-solutions::9ZdYPGXy",
            messages=[
                {"role": "system", "content": "You are an expert SQL query writer for Snowflake databases. Resolve SQL errors using the provided schema and conversation context. "},
                {"role": "user", "content": prompt}
            ],
            stream=True,
        )
        
        corrected_sql_query = ""
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                corrected_sql_query += chunk.choices[0].delta.content
                #st.write(chunk.choices[0].delta.content)  # Displaying the stream content in real-time in Streamlit
        return corrected_sql_query.strip()
    except Exception as e:
        st.error(f"Error correcting SQL: {e}")
        return ""

def generate_chart_code(dataframe):
    if isinstance(dataframe, str):
        st.error("Invalid dataframe provided to generate_chart_code.")
        return ""

    prompt = f"""
    You are an expert in data visualization. Given a pandas DataFrame with the following columns: {', '.join(dataframe.columns)}, generate the best charting code using Plotly. The code should produce an informative and visually appealing chart.

    Data to be plotted:
    {dataframe.head().to_string(index=False)}
    """

    full_prompt = [
        {"role": "system", "content": "You are an expert in data visualization using Plotly. Use the given DataFrame, identify x axis and y axis properly. you can give multiple charts if one is not enough for the data. generate professional and appealing chart code. The DataFrame will be provided as 'df'."},
        {"role": "user", "content": prompt}
    ]

    try:
        response = client.chat_completions.create(
            model="gpt-3.5-turbo",
            messages=full_prompt,
            max_tokens=4000,
            temperature=0.5,
            n=1,
            stop=None
        )
        return response.choices[0].message['content'].strip()
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

            if isinstance(result, str) and "SQL compilation error" in result:
                st.error(f"SQL compilation error: {result}")
                corrected_sql_query = handle_error(actual_sql_query, result)
                st.session_state.messages.append({"role": "assistant", "content": corrected_sql_query})
                corrected_sql_query_text = extract_query_from_message(corrected_sql_query)
                result = execute_query(corrected_sql_query_text)
                if isinstance(result, str) and "SQL compilation error" in result:
                    st.error(f"Error executing corrected query: {result}")
                else:
                    st.write("### Corrected Query Result")
                    st.write(result)
                    st.session_state.messages.append({"role": "assistant", "content": corrected_sql_query_text})
            else:
                st.write("### Query Result")
                st.session_state.messages.append({"role": "assistant", "content": result})

                if not isinstance(result, pd.DataFrame):
                    st.error("Result is not a valid DataFrame.")
                else:
                    chart_code_response = generate_chart_code(result)
                    st.write("### Chart Code Response")
                    chart_code = extract_code_from_response(chart_code_response)
                    try:
                        # Define the local scope for exec to capture the figure
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
                if isinstance(result, pd.DataFrame):
                    fig = px.line(result)
                    st.plotly_chart(fig)
            except:
                print("Something is suspicious")
            st.code(message['content'], language='sql')
            if isinstance(result, pd.DataFrame):
                st.write(result)
else:
    st.warning("Please enter your OpenAI API key to proceed.")
