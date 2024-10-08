import pandas as pd
import google.generativeai as genai
import streamlit as st
import os
import snowflake.connector
import plotly.express as px
import re
from plotly.subplots import make_subplots
import plotly.graph_objs as go

# Ensure session state is initialized at the very beginning
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Check and prompt for Snowflake credentials
SNOWFLAKE_PASSWORD = st.secrets.credentials.sf_password
SNOWFLAKE_USER = "DATAINTEGRITY_KALIPER"
SNOWFLAKE_ACCOUNT = "jsgkebp-cn71497"
SNOWFLAKE_DATABASE = "RUDDER_EVENTS"
SNOWFLAKE_WAREHOUSE = "RUDDER_WAREHOUSE"
SNOWFLAKE_ROLE = "RUDDER"

genai_api_key="AIzaSyCl9G0JmmxkiUDbJg0GWSe_MJt-QWOanDs"
# Configure Gemini API key
genai.configure(api_key="AIzaSyCl9G0JmmxkiUDbJg0GWSe_MJt-QWOanDs")

# Initialize your Gemini model
model = genai.GenerativeModel('gemini-1.5-flash')

def execute_query(query):
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
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

    response = model.generate_content(prompt)
    
    return response.text.strip()

def handle_error(query, error):
    prompt = f"""
    Given the following SQL, and the error from Snowflake, along with user conversation. Resolve this. Also add 'Generated SQL Query:' term just before sql query to identify, don't add any other identifier like 'sql' or '`' in response.
    Error:
    {error}
    Code:
    {query}
    Conversation:
    {conversation}
    """

    response = model.generate_content(prompt)
    
    return response.text.strip()

def extract_query_from_message(content):
    if "Generated SQL Query:" in content:
        query_part = content.split("Generated SQL Query:", 1)[1].strip()
        
        # Remove any triple backticks and surrounding whitespace
        query_part = query_part.replace("```sql", "").replace("```", "").strip()

        # Return the cleaned query
        return query_part
    return content.replace("```sql", "").replace("```", "").strip()


def generate_chart_code(dataframe):
    if isinstance(dataframe, pd.DataFrame):
        columns_list = ', '.join(dataframe.columns)
        dataframe_str = dataframe.to_string()
        prompt = f"""
        You are an expert in data visualization. Given a pandas DataFrame with the following columns: {columns_list}, generate the best charting code using Plotly. The code should produce an informative and visually appealing chart.
        Data to be plotted:
        {dataframe_str}
        """

        response = model.generate_content(prompt)
        return response.text.strip()
    else:
        raise ValueError("The input is not a valid pandas DataFrame")

def extract_code_from_response(response):
    # Use regex to extract code block between ```python and ```
    code_block = re.search(r'```python(.*?)```', response, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()
    return ""

if genai_api_key:
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
            
            if isinstance(result, pd.DataFrame):
                st.write("### Query Result")
                st.write(result)
                st.session_state.messages.append({"role": "assistant", "content": result})
                
                # Generate and display the chart
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
            else:
                st.error(f"SQL compilation error: {result}")
                corrected_sql_query = handle_error(actual_sql_query, result)
                st.session_state.messages.append({"role": "assistant", "content": corrected_sql_query})
                corrected_sql_query_text = extract_query_from_message(corrected_sql_query)
                result = execute_query(corrected_sql_query_text)
                if isinstance(result, pd.DataFrame):
                    st.write("### Corrected Query Result")
                    st.write(result)
                    st.session_state.messages.append({"role": "assistant", "content": corrected_sql_query_text})
                    
                    # Generate and display the chart
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
                else:
                    st.error(f"Error executing corrected query: {result}")
    # Display chat history
    st.write("### Chat History")
    for message in reversed(st.session_state.messages):
        if message['role'] == 'user':
            st.write(f"**User:** {message['content']}")
        else:
            st.write(f"**Assistant:**")
            try:
                fig = px.line(result)  # Example chart, customize based on your data
                st.plotly_chart(fig)
            except:
                st.write("...")
            st.code(message['content'], language='sql')
            st.write(result)
else:
    st.warning(f"Please enter your Gemini API key to proceed.")
