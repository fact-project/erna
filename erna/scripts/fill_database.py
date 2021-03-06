import pandas as pd
from dateutil import parser as date_parser
import click

from ..automatic_processing import fill_data_runs, fill_drs_runs
from ..automatic_processing.database import database, setup_database
from ..utils import load_config
from ..utils import create_mysql_engine


def parse_date(date):
    d = date_parser.parse(date)
    return 10000 * d.year + 100 * d.month + d.day


query = '''
SELECT {columns}
FROM RunInfo
JOIN RunType
ON RunType.fRunTypeKey = RunInfo.fRunTypeKey
WHERE
    fNight >= {start}
    AND fNight < {end}
;
'''


@click.command()
@click.argument('start', type=parse_date)
@click.argument('end', type=parse_date)
@click.option('--config', '-c', help='yaml config file with database credentials')
def main(start, end, config):
    '''
    Fill RawDataFile and DrsFile tables from the FACT RunInfo database
    from START to END.
    '''

    config = load_config(config)

    database.init(**config['processing_database'])
    database.connect()
    setup_database(database, drop=False)

    factdb = create_mysql_engine(**config['fact_database'])
    with factdb.connect() as conn:
        runs = pd.read_sql_query(
            query.format(
                columns=', '.join([
                    'fNight', 'fRunID', 'fDrsStep', 'fROI',
                    'RunInfo.fRunTypeKey AS fRunTypeKey',
                    'fRunTypeName'
                ]),
                start=start,
                end=end,
            ),
            conn,
        )
    runs['fNight'] = pd.to_datetime(runs.fNight.astype(str), format='%Y%m%d')

    # fill all non drs runs into raw_data_files
    data_runs = runs.query('fDrsStep != 2').drop('fDrsStep', axis=1)
    nan_entries = data_runs.isnull().any(axis=1)
    if nan_entries.sum() > 0:
        print('Found invalid data runs, skipping:')
        print(data_runs[nan_entries])
        data_runs.dropna(inplace=True)

    fill_data_runs(data_runs, database=database)

    # fill all drs runs into drs_files
    drs_runs = runs.query('(fRunTypeKey == 2) & (fDrsStep == 2)')
    nan_entries = drs_runs.isnull().any(axis=1)
    if nan_entries.sum() > 0:
        print('Found invalid drs runs, skipping:')
        print(drs_runs[nan_entries])
        drs_runs.dropna(inplace=True)
    fill_drs_runs(drs_runs, database=database)

    database.close()


if __name__ == '__main__':
    main()
