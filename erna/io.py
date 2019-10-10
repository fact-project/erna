import logging
import os
import pandas as pd
import json

from dask.distributed import as_completed
from traceback import format_tb

from fact.io import to_h5py
from .hdf_utils import rename_columns
from .features import add_theta_deg_columns


logger = logging.getLogger(__name__)


def read_facttools_json(json_path):
    with open(json_path, 'r') as text:
        logger.info("Reading fact-tools output.")
        y = json.loads(text.read())

    df_out = pd.DataFrame(y)
    logger.info("Returning data frame with {} entries".format(len(df_out)))
    return df_out


class Writer:
    def __init__(self, outputfile, fmt=None):

        self.outputfile = outputfile
        self._file = None

        if self.outputfile is None:
            return

        if fmt is None:
            name, ext = os.path.splitext(outputfile)

            if ext not in ['.jsonl', '.jsonlines', '.h5', '.hdf5', '.hdf', '.csv']:
                logger.warn('Did not recognize ext {}. Writing to hdf5'.format(ext))
                ext = '.hdf5'
                fmt = 'hdf5'
            elif ext in ('.jsonl', '.jsonlines'):
                logger.info('Writing JSONLines to {}'.format(outputfile))
                fmt = 'jsonl'
            elif ext in ['.h5', '.hdf', '.hdf5']:
                logger.info('Writing HDF5 to {}'.format(outputfile))
                fmt = 'hdf5'
            elif ext == '.csv':
                logger.info('Writing CSV to {}'.format(outputfile))
                fmt = 'csv'

            outputfile = name + ext
        else:
            if fmt not in {'csv', 'jsonl', 'hdf5'}:
                raise ValueError('unsupported format: {}'.format(fmt))

        self.fmt = fmt
        self.header_written = False

    def append(self, df):
        if self.outputfile is None:
            return

        if self.fmt == 'jsonl':
            if self._file is None:
                self._file = open(self.outputfile, 'w')
            df.to_json(self._file, lines=True, date_format='iso', orient='records')
            self._file.write('\n')

        elif self.fmt == 'csv':
            if self._file is None:
                self._file = open(self.outputfile, 'w')
            df.to_csv(self._file, header=not self.header_written)

        elif self.fmt == 'hdf5':
            mode = 'a' if self.header_written else 'w'
            to_h5py(df, self.outputfile, key='events', mode=mode)

        self.header_written = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._file is not None:
            self._file.close()


def collect_output(futures, output_path, **kwargs):
    '''
    Collects the output from the list of futures and merges them into a dataframe.
    The Dataframe will then be written to a file as specified by the output_path.
    The datatframe df_started_runs is joined with the job outputs to get the real ontime.
    '''
    if output_path is not None:
        logger.info('Concatenating results from each job into {}'.format(output_path))

    n_success = 0
    n_total = 0

    result_iterator = as_completed(futures, with_results=True, raise_errors=False)
    with Writer(output_path) as writer:
        for (future, result) in result_iterator:

            if isinstance(result, tuple):
                exc_type, exc_valye, tb = result
                logger.error('Exception running job: {}'.format(result[1]))
                logger.error('\n'.join(format_tb(tb)))
                continue

            if not result['success']:
                logger.error('Job errored with reason "{}"'.format(result["reason"]))
                continue

            n_success += 1
            events = result.get('events')

            if events is None:
                output = result['outputfile']
                logger.info('Job wrote output to local file {}'.format(output))
                continue

            n_events = len(events)
            logger.info('There are {} events in the result'.format(n_events))

            if n_events == 0:
                continue

            n_total += n_events

            events.columns = rename_columns(events.columns)
            add_theta_deg_columns(events)
            writer.append(events)

            logger.info('Result written successfully')

    if output_path is not None:
        logger.info('Wrote a total of {} events from {} succesfull runs to {}'.format(
            n_total, n_success, output_path
        ))
