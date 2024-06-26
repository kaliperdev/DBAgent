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

openai_api_key = st.secrets.credentials.api_key
client = OpenAI(api_key = openai_api_key)
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
        {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards. These queries should follow format from examples and Schema file. Query should not be out of schema provided, this is most crucial, especially make sure of schema when you are giving join statements with ON clause, and filters. Dont Assume.  Also add 'Generated SQL Query:' term just before sql query to identify, don't add any other identifier, apart from text 'Generated SQL Query:', and don't write anything after the query ends."},
        {"role": "user", "content": prompt}
    ]

    # Tokenizer
    enc = tiktoken.get_encoding("cl100k_base")

    # Calculate token count for each message
    token_count = sum([len(enc.encode(message["content"])) for message in full_prompt])
    st.write(token_count)
    # Print token count and full prompt for debugging
    print(f"Total token count: {token_count}")
    print("Full prompt being sent:")
    #st.write(full_prompt)
    
    # # Ensure token count is within the model's limit
    # if token_count > 4096:  # Adjust based on model's token limit (e.g., 4096 for GPT-4)
    #     raise ValueError("Prompt is too long and exceeds the token limit for the model.")
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=full_prompt,
        max_tokens=4000,
        temperature=0.5,
        n=1,
        stop=None
    )
    return response.choices[0].message.content.strip()

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
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a Snowflake Expert that generates SQL queries. Use Snowflake processing standards. Also add 'Generated SQL Query:' term just before sql query to identify, don't add any other identifier like 'sql' or '`' in response, apart from text 'Generated SQL Query:' and don't write anything after the query ends."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=4000,
        temperature=0.5,
        n=1,
        stop=None
    )
    return response.choices[0].message.content.strip()

def extract_query_from_message(content):
    if "Generated SQL Query:" in content:
        query_part = content.split("Generated SQL Query:", 1)[1].strip()
        
        # Case 2 and 3: Handle queries enclosed in triple backticks
        if query_part.startswith("```sql") and query_part.endswith("```"):
            return query_part[6:-3].strip()
        elif query_part.startswith("```") and query_part.endswith("```"):
            return query_part[3:-3].strip()
        
        # Case 1: Plain query after the "Generated SQL Query:" string
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

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=full_prompt,
        max_tokens=4000,
        temperature=0.5,
        n=1,
        stop=None
    )
    return response.choices[0].message.content.strip()

def extract_code_from_response(response):
    # Use regex to extract code block between ```python and ```
    code_block = re.search(r'```python(.*?)```', response, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()
    return ""


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

    st.title("BI Automation")

    # Streamlit interface
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
                #st.write(result)
                st.session_state.messages.append({"role": "assistant", "content": result})
                # Generate and display the chart

                chart_code_response = generate_chart_code(result)
                st.write("### Chart Code Response")
                # st.write(chart_code_response)
                chart_code = extract_code_from_response(chart_code_response)
                #st.code(chart_code, language='python')
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

    # Display chat history
    st.write("### Chat History")
    for message in reversed(st.session_state.messages):
        if message['role'] == 'user':
            st.write(f"**User:** {message['content']}")
        else:
            #st.write(f"**Assistant:** {message['content']}")
            try:
                fig = px.line(result)  # Example chart, customize based on your data
                st.plotly_chart(fig)
            except:
                print("Something is Suspicious")
            st.code(message['content'], language='sql')
            st.write(result)
else:
    st.warning(f"Please enter your OpenAI API key to proceed. {st.secrets.credentials.sf_password}")
