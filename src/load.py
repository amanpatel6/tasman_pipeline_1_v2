import pandas as pd
import pathlib
import dlt
import duckdb

def load():
    input_path = pathlib.Path("bronze/raw_breweries.json")

    df = pd.read_json(input_path)

    assert "id" in df.columns, "Missing id column"
    assert df["id"].isnull().sum() == 0, "Nulls found in id column"
    assert df.shape != (0, 0), "Dataframe is empty"

    # create a dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="breweries_v2", # The name dlt gives this pipeline. It also becomes the name of the .duckdb file that gets created
        destination="duckdb", # Tells dlt to load the data into DuckDB
        dataset_name="silver" # this becomes the schema name inside duckDB. 
    )

    # run the pipeline
    load_info = pipeline.run(df, table_name="breweries_v2", write_disposition="replace")
    print(load_info)

    # Verify the data is actually in DuckDB
    conn = duckdb.connect("breweries_v2.duckdb")
    print(conn.execute("SELECT COUNT(*) FROM silver.breweries_v2").fetchone())
    conn.close()

if __name__ == "__main__": load()