# main.py
import os
import streamlit as st
import pandas as pd
from get_data import get_dataframe
from dotenv import load_dotenv
from graph_functions import makeTreemap, makeSunkey, makeIcicle, makeSunburst

load_dotenv()

#print(os.environ.get("AZURE_STORAGE_ACCOUNT"))

def main():
    STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT")
    CREDENTIAL = os.environ.get("AZURE_STORAGE_KEY")
    FILE_SYSTEM_NAME = "appdata"
    DIRECTORY_PATH = "employees"
    FILE_NAME = "employees.csv"
    
    df = get_dataframe(
        storage_account_name=STORAGE_ACCOUNT_NAME,
        credential=CREDENTIAL,
        file_system_name=FILE_SYSTEM_NAME,
        directory_path=DIRECTORY_PATH,
        file_name=FILE_NAME
    )
    
    #This is a change
    # Now you can work with df here
    
    df["EMPLOYEE_ID"] = df["EMPLOYEE_ID"].astype(str)

    # Add full name
    df["FULL_NAME"] = df["FIRST_NAME"].astype(str) + " " + df["LAST_NAME"].astype(str)

    # Create a mapping from EMPLOYEE_ID to FULL_NAME
    emp_id_to_name = df.set_index("EMPLOYEE_ID")["FULL_NAME"].to_dict()

    # Map MANAGER_ID to manager name, handling missing or invalid manager IDs
    df["MANAGER_ID"] = df["MANAGER_ID"].astype(str)
    df["MANAGER_NAME"] = df["MANAGER_ID"].map(emp_id_to_name)

    #print(df.head())


    st.title("Hierarchical Data Viewer")

    labels = df[df.columns[11]]
    parents = df[df.columns[12]]

    fig = makeTreemap(labels, parents)
    st.plotly_chart(fig, use_container_width=True)

    edges = ""
    for _, row in df.iterrows():
        if not pd.isna(row.iloc[12]):
            edges += f'\t"{row.iloc[11]}" -> "{row.iloc[12]}";\n'



    d = f'''
    digraph {{
        rankdir=LR;
        {edges}
    }}
    '''

    st.graphviz_chart(d)


if __name__ == "__main__":
    main()




