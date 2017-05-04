import os
from os.path import isfile
import socket
import click
import logging
import pandas as pd
from dateutil.parser import parse as parse_date
import datetime
from sqlalchemy import create_engine
from tqdm import tqdm

from ..automatic_processing.database import RawDataFile, DrsFile, database
from ..automatic_processing.custom_fields import night_int_to_date
from ..utils import load_config

log = logging.getLogger('erna')

handler = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s|%(levelname)s|%(name)s|%(message)s')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)

db_specification = 'mysql+pymysql://{user}:{password}@{host}/{database}'


run_query = '''
SELECT fNight AS night, fRunID AS run_id, fRunTypeKey AS run_type, fDrsStep AS drs_step
FROM RunInfo
WHERE (fNight >= {start:%Y%m%d} and fNight <= {end:%Y%m%d});
'''


def check_availability(run, basedir='/fact/raw'):
    log.debug('Checking run {:%Y%m%d}_{:03d}'.format(run.night, run.run_id))

    basename = os.path.join(
        basedir, str(run.night.year), '{:02d}'.format(run.night.month),
        '{:02d}'.format(run.night.day), '{:%Y%m%d}_{:03d}'.format(run.night, run.run_id)
    )

    if run.drs_step == 2:
        log.debug('is a drs file')

        try:
            f = DrsFile.get(night=run.night, run_id=run.run_id)
            available = isfile(basename + '.drs.fits.gz')
            log.debug('Available: {}'.format(available))
            if available != f.available:
                f.available = available
                f.save()
        except DrsFile.DoesNotExist:
            log.info('DrsFile {:%Y%m%d}_{:03d} not in database'.format(
                run.night, run.run_id
            ))

    else:
        try:
            f = RawDataFile.get(night=run.night, run_id=run.run_id)
            available = isfile(basename + '.fits.fz') or isfile(basename + '.fits.gz')
            log.debug('Available: {}'.format(available))

            if available != f.available:
                f.available = available
                f.save()
        except RawDataFile.DoesNotExist:
            log.info('Run {:%Y%m%d}_{:03d} not in database'.format(
                run.night, run.run_id
            ))


@click.command()
@click.option('--config', '-c', help='Yaml file containing database credentials')
@click.option('--verbose', '-v', help='Set logging level to DEBUG', is_flag=True)
@click.option('--start', type=parse_date, default=str(datetime.date(2011, 10, 1)))
@click.option('--end', type=parse_date, default=str(datetime.date.today()))
def main(config, verbose, start, end):
    '''
    Check if RawDataFiles and DrsFiles are available.
    Goes through the database entries and checks if the file is where it is expected
    to be.
    '''

    if verbose:
        log.setLevel(logging.DEBUG)
        logging.captureWarnings(True)

    config = load_config(config)

    log.debug('Connecting to database')
    database.init(**config['processing_database'])
    database.connect()
    log.info('Database connection established')

    basedir = '/fact/raw'

    fact_db = create_engine(db_specification.format(**config['fact_database']))
    runs = pd.read_sql_query(run_query.format(start=start, end=end), fact_db)
    runs['night'] = runs['night'].apply(night_int_to_date)

    log.info('basedir is: {}'.format(basedir))

    runs = runs.query('run_type == 1 or (run_type == 2 and drs_step == 2)')

    log.info('Checking data files')

    for run in tqdm(runs.itertuples(), total=len(runs)):
        check_availability(run, basedir=basedir)

    database.close()


if __name__ == '__main__':
    main()
