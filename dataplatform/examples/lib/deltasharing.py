import json
from delta_sharing import SharingClient, load_as_pandas
from typing import List

def create_sharing_profile(
        bearer_token: str,
        endpoint: str,
        profile_name: str = "profile",
    ) -> str:
    """
    Create a Delta Sharing profile JSON file.
    
    returns the filename of the created profile.
    """

    # Create a SharingClient instance using the access token
    profile = {
        "shareCredentialsVersion": 1,
        "bearerToken": bearer_token,
        "endpoint": endpoint
    }

    profile_filename = f"{profile_name}.json"

    with open(f"{profile_filename}", "w") as f:
        json.dump(profile, f)
        
    return profile_filename


def get_table_urls(profile: str, sharing_client: SharingClient):
    """
    Convert a list of Delta Sharing tables to resource URLs.
    """

    tables = sharing_client.list_all_tables()
    table_list = [f"{profile}#{table.share}.{table.schema}.{table.name}" for table in tables]

    return table_list

def pandas_dump_tables(
        table_urls: List[str],
        data_folder: str = "data",
):
    """
    Load Delta Sharing tables as Pandas DataFrames and save them to disk.
    """
    import time
    for url in table_urls:
        start_time = time.time()
        print(f"Loading table: {url}")
        df = load_as_pandas(url=url)
        # Create the data folder if it doesn't exist
        import os
        os.makedirs(data_folder, exist_ok=True)
        # Save the DataFrame to a Parquet file
        df.to_parquet(f"{data_folder}/{'.'.join(url.split('#')[1:])}.parquet")
        end_time = time.time()
        print(f"Table {url} loaded in {end_time - start_time:.2f} seconds.")