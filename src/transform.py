import subprocess
import pathlib

def transform():
    dbt_path = pathlib.Path("breweries_v2")
    
    subprocess.run(["dbt", "run"], cwd=dbt_path)
    subprocess.run(["dbt", "test"], cwd=dbt_path)

if __name__ == "__main__": transform()