

# Display conversation history
import pandas as pd
import openai
import streamlit as st

# Initialize OpenAI API
openai.api_key = 'sk-gpt-service-account-OLZzNlZl96FVr8Uxa3QjT3BlbkFJD24Chu1eRPerwHmWPYra'

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
        # Add user question to conversation history
        st.session_state['messages'].append({"role": "user", "content": user_question})

        # Generate SQL query based on the conversation history
        conversation = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state['messages']])
        sql_query = generate_sql(conversation)

        # Add the generated SQL query to the conversation history
        st.session_state['messages'].append({"role": "assistant", "content": sql_query})

        # Display the generated SQL query
        st.write("### Generated SQL Query")
        st.code(sql_query, language='sql')
    else:
        st.write("Please enter a question to generate an SQL query.")


