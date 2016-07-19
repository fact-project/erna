import pandas as pd
from fact.credentials import create_factdb_engine
from dateutil import parser as date_parser
import click

from ..database import RawDataFile, DrsFile, database, init_database


def parse_date(date):
    d = date_parser.parse(date)
    return 10000 * d.year + 100 * d.month + d.day


query = '''
SELECT {columns}
FROM RunInfo
WHERE
    fNight >= {start}
    AND fNight < {end}
    AND (
        fRunTypeKey = 1 OR (fDrsStep = 2 AND fRunTypeKey = 2)
    )
;
'''


@click.command()
@click.argument('start', type=parse_date)
@click.argument('end', type=parse_date)
def main(start, end):
    init_database(drop=False)
    runs = pd.read_sql_query(
        query.format(
            columns=', '.join(['fNight', 'fRunID', 'fDrsStep', 'fRunTypeKey']),
            start=start,
            end=end,
        ),
        create_factdb_engine(),
    )
    runs['fNight'] = pd.to_datetime(runs.fNight.astype(str), format='%Y%m%d')

    for runtype, df in runs.groupby('fRunTypeKey'):
        if runtype == 1:
            fill_data_runs(df, database=database)
        elif runtype == 2:
            fill_drs_runs(df, database=database)


def fill_data_runs(runs, database):
    runs.rename(columns={'fNight': 'night', 'fRunID': 'run_id'}, inplace=True)
    runs.drop(['fDrsStep', 'fRunTypeKey'], axis=1, inplace=True)
    with database.atomic():
        RawDataFile.insert_many(runs.to_dict(orient='records')).execute()


def fill_drs_runs(runs, database):
    runs.rename(columns={'fNight': 'night', 'fRunID': 'run_id'}, inplace=True)
    runs.drop(['fDrsStep', 'fRunTypeKey'], axis=1, inplace=True)
    with database.atomic():
        DrsFile.insert_many(runs.to_dict(orient='records')).execute()
