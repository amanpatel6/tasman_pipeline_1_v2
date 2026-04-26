import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import pandas as pd

    conn = duckdb.connect("/Users/amanpatel/Desktop/tasman_pipeline_v2/breweries_v2.duckdb")
    df = conn.execute("SELECT * FROM main.mart_brewery_by_state").df()
    df.head()
    return (df,)


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Q1 Which state has the most breweries?
        SELECT
        state_province,
        MAX(breweries)

        FROM df

        GROUP BY
        state_province

        ORDER BY 2 DESC

        LIMIT 1
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Q2 Which state has the most micro breweries?
        SELECT
        state_province,
        MAX(micro_breweries)
    
        FROM df

        GROUP BY
        state_province

        ORDER BY 
        2 DESC

        LIMIT 1
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Q3 What percentage of all breweries are closed?
        SELECT 
        ROUND(SUM(closed_breweries) * 100.0 / SUM(breweries), 2) as pct_closed

        FROM df
        """
    )
    return


if __name__ == "__main__":
    app.run()
