import pandas as pd
import openai
import streamlit as st

# Initialize OpenAI API
openai.api_key = 'sk-gpt-service-account-OLZzNlZl96FVr8Uxa3QjT3BlbkFJD24Chu1eRPerwHmWPYra'

# Load schema CSV
schema_file_path = '/Users/sunnykumar/Downloads/Schema.csv'
schema_df = pd.read_csv(schema_file_path)

# Load examples CSV
examples_file_path = '/Users/sunnykumar/Downloads/Examples.csv'
examples_df = pd.read_csv(examples_file_path)

# # Load additional documentation
# documentation_file_path = '/Users/sunnykumar/Downloads/Documentation.txt'
# with open(documentation_file_path, 'r') as doc_file:
#     documentation_content = doc_file.read()

# Prepare schema information
schema_info = ""
for _, row in schema_df.iterrows():
    schema_info += f"Table: {row['Table Name']}\nColumn: {row['Column Name']}\nDescription: {row['Column Description']}\n\n"

# Prepare examples
examples = ""
for _, row in examples_df.iterrows():
    examples += f"Question: {row['Question']}\nQuery: {row['Query']}\n\n"

def generate_sql(query):
    prompt = f"""
    You are an expert SQL query writer. Given the following schema, examples, and additional documentation, generate a SQL query for the given question. Be mindful of the following: 1. The query should only contain tables and columns as per the schema. For help in generating the query, refer to the examples and additional documentation.

    Schema:
    {schema_info}

    Examples:
    {examples}

    # Additional Documentation:
    # documentation_content

    Question: {query}
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",  # Use the GPT-4 model for chat completions
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates SQL queries."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1000,
        temperature=0.5,
        n=3,
        stop=None
    )
    return [choice['message']['content'].strip() for choice in response.choices]


# # Streamlit interface
# st.title("SQL Query Generator")

# user_question = st.text_input("Enter your question:")

# if st.button("Generate SQL Query"):
#     if user_question:
#         sql_queries = generate_sql(user_question)
        
#         st.write("### Generated SQL Queries")
#         for i, query in enumerate(sql_queries, start=1):
#             st.write(f"**SQL Query {i}:**")
#             st.code(query, language='sql')
#     else:
#         st.write("Please enter a question to generate SQL queries.")


# Example usage
user_question = "What is the total Current Cycle Deals for the May month in year 2024, for the Funnel: ATA D2C Opt-In?"
sql_queries = generate_sql(user_question)

# Save the top 3 responses to a file
output_file = 'generated_sql_queries.txt'
with open(output_file, 'w') as file:
    for i, query in enumerate(sql_queries, start=1):
        file.write(f"SQL Query {i}:\n{query}\n\n")

print(f"The SQL queries have been saved to {output_file}")
for i, query in enumerate(sql_queries, start=1):
    print(f"SQL Query {i}:\n{query}\n")
