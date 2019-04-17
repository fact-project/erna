from fact.credentials import create_factdb_engine
import pandas as pd
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--min-current', default=0, type=float)
parser.add_argument('--max-current', default=10, type=float)
args = parser.parse_args()


db = create_factdb_engine()

with open('drs_query.sql') as f:
    query = f.read().format(args.min_current, args.max_current)

runs = pd.read_sql(query, db)
runs.to_csv(f'runs_{args.min_current}_{args.max_current}.csv', index=False)
