import ocha_stratus as stratus
import duckdb

def get_emdat_data(iso3, disaster_type):
    # get blob URL - note that this contains the SAS so be careful
    blob_name = "emdat/processed/emdat_all.parquet"
    url = (
        stratus.get_container_client(container_name="global")
        .get_blob_client(blob_name)
        .url
    )

    # load using duckdb
    con = duckdb.connect()
    df_in = con.execute(
        f"""
        SELECT *
        FROM read_parquet('{url}')
        WHERE ISO = '{iso3}' AND "Disaster Type" = '{disaster_type}'
    """
    ).df()
    return df_in