# main.py
import os
from get_data import get_dataframe
from dotenv import load_dotenv

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
    
    # Now you can work with df here
    print(df.head())

if __name__ == "__main__":
    main()




