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
        format: str = "parquet",
        csv_separator: str = ","
):
    """
    Load Delta Sharing tables as Pandas DataFrames and save them to disk.
    """
    import time
    
    for index, url in enumerate(table_urls):
        table_name = url.split('#')[1]
        start_time = time.time()
        print(f"Loading table '{table_name}' ({index + 1}/{len(table_urls)})")
        df = load_as_pandas(url=url)
        # Create the data folder if it doesn't exist
        import os
        os.makedirs(data_folder, exist_ok=True)
        # Save the DataFrame to disk
        if format == "csv":
            df.to_csv(
                path_or_buf=f"{data_folder}/{table_name}.csv",
                sep=csv_separator,
                index=False
            )
        elif format == "parquet":
            df.to_parquet(
                path=f"{data_folder}/{table_name}.parquet",
            )
        elif format == "excel":
            # Remove timezone from datetime columns
            for col in df.select_dtypes(include=['datetime64[ns, UTC]']).columns:
                df[col] = df[col].dt.tz_localize(None)
            # Save to Excel
            df.to_excel(
                excel_writer=f"{data_folder}/{table_name}.xlsx",
                index=False
            )
        end_time = time.time()
        print(f"Completed in {end_time - start_time:.2f} seconds.\n")