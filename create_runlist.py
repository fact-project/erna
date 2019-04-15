from fact.credentials import create_factdb_engine
import pandas as pd


db = create_factdb_engine()

with open('drs_query.sql') as f:
    query = f.read()


runs = pd.read_sql(query, db)

runs.to_csv('all_runs.csv', index=False)
