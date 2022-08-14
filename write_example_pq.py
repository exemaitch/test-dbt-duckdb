import os
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_table_to_s3():
    df = pd.DataFrame(
        {
            'id': [1, 2, 3],
            'name': ['Jane', 'Joe', 'Brenda'],
            'age': [25, 43, 47]
        }
    )
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 's3://test-dbt-duckdb/example.parquet')


def read_parquet_from_s3():
    s3_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("INSTALL 'httpfs'")
    con.execute("LOAD 'httpfs'")
    con.execute("SET s3_region='us-east-1'")
    con.execute(f"SET s3_access_key_id='{s3_access_key_id}'")
    con.execute(f"SET s3_secret_access_key='{s3_secret_access_key}'")
    results = con.execute("SELECT * FROM read_parquet('s3://test-dbt-duckdb/example.parquet')").fetchall()
    print(results)

if __name__ == '__main__':
    write_table_to_s3()
    read_parquet_from_s3()
