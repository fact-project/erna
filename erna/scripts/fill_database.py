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
    AND (
        RunInfo.fRunTypeKey = 1 OR (fDrsStep = 2 AND RunInfo.fRunTypeKey = 2)
    )
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

    runs = pd.read_sql_query(
        query.format(
            columns=', '.join([
                'fNight', 'fRunID', 'fDrsStep', 'RunInfo.fRunTypeKey AS fRunTypeKey', 'fRunTypeName'
            ]),
            start=start,
            end=end,
        ),
        create_mysql_engine(**config['fact_database']),
    )
    runs['fNight'] = pd.to_datetime(runs.fNight.astype(str), format='%Y%m%d')

    # fill all non drs runs into raw_data_files
    fill_data_runs(runs.query('fDrsStep != 2'), database=database)
    # fill all drs runs into drs_files
    fill_drs_runs(runs.query('(fRunTypeKey == 2) & (fDrsStep == 2)'), database=database)

    database.close()


if __name__ == '__main__':
    main()
