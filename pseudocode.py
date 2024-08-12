import pandas as pd
import openai
import streamlit as st
import os

# Ensure session state is initialized at the very beginning
if 'messages' not in st.session_state:
    st.session_state.messages = []

openai.api_key = st.secrets.credentials.api_key

def generate_pseudocode(conversation):
    prompt = f"""
    You are an expert at generating step-wise pseudocode for SQL generation. Given the following schema and examples, generate pseudocode for the given question in steps. Each step should clearly define actions like selecting columns, specifying table names, applying filters, and joining tables. The pseudocode should be formatted so that it can later be used to generate SQL queries.
    Schema:
    {schema_info}
    Examples:
    {examples}
    Conversation:
    {conversation}
    """
    response = openai.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a Pseudocode Expert who generates step-wise pseudocode for SQL generation."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1000,
        temperature=0.5
    )
    return response.choices[0].message.content.strip()

def clean_up_schema(schema_df):
    # Example function to clean up schema data if needed
    schema_info = ""
    for _, row in schema_df.iterrows():
        schema_info += f"Table: {row['Table Name']}\nColumn: {row['Column Name']}\nDescription: {row['Column Description']}\n\n"
    return schema_info

def clean_up_examples(examples_df):
    # Example function to clean up example data if needed
    examples = ""
    for _, row in examples_df.iterrows():
        examples += f"Question: {row['Question']}\nPseudocode: {row['Pseudocode']}\n\n"
    return examples

if openai.api_key:
    # Load schema CSV
    schema_file_path = 'Schema.csv'
    schema_df = pd.read_csv(schema_file_path)
    # Load examples CSV
    examples_file_path = 'Examples.csv'
    examples_df = pd.read_csv(examples_file_path)

    # # Prepare schema and examples information
    # schema_info = clean_up_schema(schema_df)
    # examples = clean_up_examples(examples_df)

    st.title("Step-wise Pseudocode Generator")

    # Streamlit interface
    user_question = st.text_input("Ask your question:")
    if st.button("Generate Pseudocode"):
        if user_question:
            conversation = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state.messages])
            pseudocode = generate_pseudocode(conversation + f"\nUser: {user_question}")
            st.session_state.messages.append({"role": "user", "content": user_question})
            st.session_state.messages.append({"role": "assistant", "content": pseudocode})
            
            # Display pseudocode and ask for validation
            st.write("### Generated Step-wise Pseudocode")
            st.code(pseudocode, language='plaintext')
            
            approval = st.radio("Do you approve the pseudocode?", ("Approve", "Disapprove"))
            if approval == "Approve":
                st.success("Thanks for the approval")
            else:
                st.warning("Please provide instructions to edit the pseudocode")
                user_instructions = st.text_area("Your instructions:")
                if st.button("Submit Instructions"):
                    st.session_state.messages.append({"role": "user", "content": user_instructions})
                    # Regenerate pseudocode based on user instructions
                    revised_pseudocode = generate_pseudocode(conversation + f"\nUser: {user_instructions}")
                    st.session_state.messages.append({"role": "assistant", "content": revised_pseudocode})
                    st.write("### Revised Step-wise Pseudocode")
                    st.code(revised_pseudocode, language='plaintext')
                    
    # Display chat history
    st.write("### Chat History")
    for message in reversed(st.session_state.messages):
        if message['role'] == 'user':
            st.write(f"**User:** {message['content']}")
        else:
            st.write(f"**Assistant:**")
            st.code(message['content'], language='plaintext')
else:
    st.warning("Please enter your OpenAI API key to proceed.")