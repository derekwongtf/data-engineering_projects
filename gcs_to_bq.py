from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os


@task(retries=3)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"pq/china/hk/hong-kong/{year}/{month:02}/"
    gcs_block = GcsBucket.load("de-project-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./for_bigquery/")
    return Path(f"./for_bigquery/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, year: int, month: int ) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-project-gcp-credential")

    df.to_gbq(
        destination_table=f"data_all.airbnb_{year}{month:02}",
        project_id="de-project-381616",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace",
    )


# @flow()
# def etl_gcs_to_bq():
#     """Main ETL flow to load data into Big Query"""
#     year = 2022
#     month = 12

#     path = extract_from_gcs(year, month)
#     # giving file extensions
#     ext = ('.parquet')
#     # iterating over directory and subdirectory to get desired result
#     for path, dirc, files in os.walk(path):
#         for name in files:
#             if name.endswith(ext):
#                 print(name)  # printing file name
#                 df = transform(f"{path}/{name}")
#                 write_bq(df, year, month)

# if __name__ == "__main__":
#     etl_gcs_to_bq()

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019
):
    
    for month in months:
        path = extract_from_gcs(year, month)
        # giving file extensions
        ext = ('.parquet')
        # iterating over directory and subdirectory to get desired result
        for path, dirc, files in os.walk(path):
            for name in files:
                if name.endswith(ext):
                    print(name)  # printing file name
                    df = transform(f"{path}/{name}")
                    write_bq(df, year, month)


if __name__ == "__main__":
    etl_parent_flow([3,6,9,12],2022)