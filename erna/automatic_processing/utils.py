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
