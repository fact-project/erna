from .models import RawDataFile, DrsFile
import re
from datetime import date


datafile_re = re.compile(r'(?:.*/)?([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{3})\.fits(?:\.[fg]z)?$')
drsfile_re = re.compile(r'(?:.*/)?([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{3})\.drs\.fits(?:\.gz)?$')


def parse_path(path):
    match = datafile_re.search(path)
    if match is None:
        match = drsfile_re.search(path)
    if match is None:
        raise IOError('File seems not to be a drs or data file')

    year, month, day, run_id = map(int, match.groups())

    return date(year, month, day), run_id


def fill_data_runs(df, database):
    df.rename(columns={'fNight': 'night', 'fRunID': 'run_id'}, inplace=True)
    df.drop(['fDrsStep', 'fRunTypeKey'], axis=1, inplace=True)
    with database.atomic():
        RawDataFile.insert_many(df.to_dict(orient='records')).upsert().execute()


def fill_drs_runs(df, database):
    df.rename(columns={'fNight': 'night', 'fRunID': 'run_id'}, inplace=True)
    df.drop(['fDrsStep', 'fRunTypeKey'], axis=1, inplace=True)
    with database.atomic():
        DrsFile.insert_many(df.to_dict(orient='records')).upsert().execute()
