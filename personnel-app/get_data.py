import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO

def get_service_client(account_name: str, credential: str):
    """
    Create a DataLakeServiceClient using an account name and credential (SAS token or access key).
    
    :param account_name: Name of the Azure Storage account (e.g., 'mystorageaccount').
    :param credential:  Account key or SAS token string.
    :return:            DataLakeServiceClient object.
    """
    # Construct the account URL, e.g. "https://mystorageaccount.dfs.core.windows.net"
    account_url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=credential)

def read_csv_from_adls_to_pandas(
    service_client: DataLakeServiceClient, 
    file_system_name: str, 
    directory_path: str, 
    file_name: str
) -> pd.DataFrame:
    """
    Reads a CSV file from Azure Data Lake Storage Gen2 and returns a pandas DataFrame.
    
    :param service_client:   A DataLakeServiceClient object.
    :param file_system_name: The file system/container name in the storage account.
    :param directory_path:   The path to the directory in which the file resides.
    :param file_name:        The CSV file name to read.
    :return:                 DataFrame containing the file data.
    """
    # Get File System Client
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    
    # Get Directory Client
    directory_client = file_system_client.get_directory_client(directory_path)
    
    # Get File Client
    file_client = directory_client.get_file_client(file_name)
    
    # Download the file
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    
    # Decode and read into a DataFrame
    s = downloaded_bytes.decode('utf-8')
    df = pd.read_csv(StringIO(s))
    
    return df


def get_dataframe(
    storage_account_name: str,
    credential: str,
    file_system_name: str,
    directory_path: str,
    file_name: str
) -> pd.DataFrame:
    """
    High-level function that creates a service client and returns the DataFrame
    from a specified CSV file in ADLS Gen2.
    
    :param storage_account_name: Azure Storage account name.
    :param credential:           Account key or SAS token string.
    :param file_system_name:     The file system/container name in the storage account.
    :param directory_path:       The path to the directory in which the file resides.
    :param file_name:            The CSV file name to read.
    :return:                     DataFrame containing the file data.
    """
    service_client = get_service_client(storage_account_name, credential)
    df = read_csv_from_adls_to_pandas(
        service_client, 
        file_system_name, 
        directory_path, 
        file_name
    )
    return df
