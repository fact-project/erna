import logging
import pandas as pd

from . import datacheck_conditions as dcc
from .path import build_filename, build_path, test_drs_path
from datetime import timedelta
import numpy as np


logger = logging.getLogger(__name__)


default_columns = (
    'fNight AS night',
    'fRunID AS run_id',
    'fSourceName AS source',
    'TIMESTAMPDIFF(SECOND, fRunStart, fRunStop) * fEffectiveOn AS ontime',
    'fZenithDistanceMean AS zenith',
    'fAzimuthMean AS azimuth',
    'fRunStart AS run_start',
    'fRunStop AS run_stop',
    'RunInfo.fRightAscension AS right_ascension',
    'RunInfo.fDeclination AS declination',
)


query_template_data = '''
SELECT {columns}
FROM RunInfo
JOIN Source
ON RunInfo.fSourceKey = Source.fSourceKey
JOIN RunType
ON RunInfo.fRunTypeKey = RunType.fRunTypeKey
WHERE {conditions}
;
'''

query_template_drs = '''
SELECT {columns}
FROM RunInfo
WHERE
    fDrsStep = 2
    AND fRoi = 300
    AND fRunTypeKey = 2
    AND {conditions}
;
'''


def get_runs(engine, conditions=None, columns=default_columns):

    if conditions is None:
        conditions = '1'
    else:
        conditions = ' AND '.join(conditions)

    query = query_template_data.format(
        columns=','.join(columns),
        conditions=conditions
    )
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


def get_drs_runs(engine, conditions, columns=('fNight', 'fRunID')):
    if conditions is None:
        conditions = '1'
    else:
        conditions = ' AND '.join(conditions)

    query = query_template_drs.format(
        columns=','.join(columns),
        conditions=conditions
    )

    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


def get_run_data(
    earliest_night,
    latest_night,
    path_to_data,
    factdb,
    source_name="Crab",
    timedelta_in_minutes=30,
    data_conditions=dcc.conditions["standard"]
):
    '''
    Given the earliest and latest night to fetch as a factnight string (as in 20141024)
    this method returns a DataFrame containing the paths to data files
    and their correpsonding .drs files.
    The maximum time difference between the data and drs files is
    specified by the timedelta_in_minutes parameter.

    Returns None if no files can be found.
    '''

    logger.debug("Table names in DB: ")
    logger.debug(factdb.table_names())

    if len(factdb.table_names()) > 0:
        logger.info("Connected to Database.")

    logger.info("Reading Data from DataBase from {} to {} for source: {}".format(
        earliest_night, latest_night, source_name
    ))

    conditions = [
        'fNight >= {}'.format(earliest_night),
        'fNight <= {}'.format(latest_night),
        'fSourceName = "{}"'.format(source_name),
    ]
    conditions.extend(data_conditions)
    logger.info('Querying data with conditions: {}'.format(' AND '.join(conditions)))
    data = get_runs(
        factdb,
        conditions=conditions,
        columns=(
            'fNight AS night', 'fRunID AS run_id',
            'fRunStart', 'fRunStop',
            'fOnTime', 'fEffectiveOn',
        ),
    )

    # now lets get all drs runs
    drs_conditions = [
        'fNight >= {}'.format(earliest_night),
        'fNight <= {}'.format(latest_night),
    ]

    drs_data = get_drs_runs(
        factdb, conditions=drs_conditions,
        columns=('fNight AS night', 'fRunID AS run_id', 'fRunStart', 'fRunStop'),
    )

    if len(data) == 0 or len(drs_data) == 0:
        logger.error('No data or drs files found that adhere to the specified query.')
        return None

    logger.info("Got {} data runs and {} runs".format(len(data), len(drs_data)))

    # the timestamp should be unique for each observation.
    # No two observations start at the same time
    data.set_index("fRunStart", inplace=True)
    drs_data.set_index("fRunStart", inplace=True)
    # sorting data by their timestamp.
    data = data.sort_index()
    drs_data = drs_data.sort_index()

    # write filenames
    data["filename"] = build_filename(data.night, data.run_id)
    drs_data["filename"] = build_filename(drs_data.night, drs_data.run_id)

    # write path
    data["path"] = data.apply(build_path, axis=1, path_to_data=path_to_data, extension='.fits.fz')
    drs_data["path"] = drs_data.apply(build_path, axis=1, path_to_data=path_to_data, extension='.drs.fits.gz')

    # remove all none existing drs files
    drs_data = test_drs_path(drs_data, "path")
    drs_data = drs_data[drs_data['drs_file_exists']]

    # reindex the drs table using the index of the data table.
    # There are always more data runs than drs run in the db.
    # hence missing rows have to be filled either forward or backwards
    earlier_drs_entries = drs_data.reindex(data.index, method="ffill")
    earlier_drs_entries = earlier_drs_entries.fillna(axis="index", method="ffill")
    later_drs_entries = drs_data.reindex(data.index, method="backfill")
    later_drs_entries = later_drs_entries.fillna(axis="index", method="ffill")

    # when backfilling the drs obeservations the last rows might be invalid and contain nans.
    # We cannot drop them becasue the tables have to have the same length.
    # in that case simply fill them up.
    earlier_drs_entries["deltaT"] = np.abs(earlier_drs_entries.fRunStop - data.fRunStop)
    later_drs_entries["deltaT"] = np.abs(later_drs_entries.fRunStop - data.fRunStop).fillna(axis='index', method='ffill')
    d_later = later_drs_entries[later_drs_entries.deltaT < earlier_drs_entries.deltaT]
    d_early = earlier_drs_entries[later_drs_entries.deltaT >= earlier_drs_entries.deltaT]

    closest_drs_entries = pd.concat([d_early, d_later])
    valid_drs = closest_drs_entries.deltaT < timedelta(minutes=timedelta_in_minutes)
    closest_drs_entries = closest_drs_entries[valid_drs]

    mapping = pd.concat([
        closest_drs_entries.filename,
        closest_drs_entries.path,
        data.path,
        closest_drs_entries.deltaT,
        data.fOnTime, data.fEffectiveOn,
        data.night,
        data.run_id,
    ], axis=1, keys=[
        "filename",
        "drs_path",
        "data_path",
        "delta_t",
        "ontime",
        "effective_on",
        "night",
        "run_id",
    ])

    mapping = mapping.dropna(how='any')

    logger.info(
        "Fetched {} data runs and approx {} drs entries from database where time delta is less than {} minutes".format(
            len(mapping), mapping['drs_path'].nunique(), timedelta_in_minutes
        )
    )
    return mapping
