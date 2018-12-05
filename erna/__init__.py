import logging
import pandas as pd
import os
import numpy as np
import datetime
import json
import pkg_resources
from datetime import timedelta
from fact.io import write_data

from fact.io import to_h5py
from fact.instrument import camera_distance_mm_to_deg

from . import datacheck_conditions as dcc
from .datacheck import get_runs, get_drs_runs, default_columns
from .hdf_utils import rename_columns
logger = logging.getLogger(__name__)


def add_theta_deg_columns(df):
    for i in range(6):
        incol = 'theta' if i == 0 else 'theta_off_{}'.format(i)
        outcol = 'theta_deg' if i == 0 else 'theta_deg_off_{}'.format(i)
        if incol in df.columns:
            df[outcol] = camera_distance_mm_to_deg(df[incol])



def build_path(row, path_to_data, extension):
    """
    builds a path to the fact data given the night, extension and filename
    """
    night = str(row.night)
    year = night[0:4]
    month = night[4:6]
    day = night[6:8]
    res = os.path.join(path_to_data, year, month, day, row.filename + extension)
    return res

def test_drs_path(df, key):
    """
    Test if the given drs paths in the key are present
    """
    mask = df[key].apply(os.path.exists)
    df['drs_file_exists'] = mask

    return df


def test_data_path(df, key):
    """
    Test the given data paths in key if they exists. It tests for
    both possible fileextensions [.fz, .gz] and corrects if necessary.
    """
    mask = df[key].apply(os.path.exists)
    df['data_file_exists'] = mask
    df.loc[~mask, key] = df.loc[~mask, key].str.replace('.fz', '.gz')
    df.loc[~mask, 'data_file_exists'] = df.loc[~mask, key].apply(os.path.exists)

    return df


def build_filename(night, run_id):
    return night.astype(str) + '_' + run_id.map('{:03d}'.format)


def mc_drs_file():
    '''
    return path to the drs file used for monte carlo files
    '''
    drs_path = pkg_resources.resource_filename(
        __name__, 'resources/mc_drs_constants.drs.fits.gz'
    )
    return drs_path


def ensure_output(output_path):
    '''
    Make sure the output file does not exist yet.
    Create directorie to new output file if necessary
    '''
    if os.path.exists(output_path):
        raise FileExistsError('The output file already exists.')
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def collect_output(job_outputs, output_path, df_started_runs=None, **kwargs):
    '''
    Collects the output from the list of job_outputs and merges them into a dataframe.
    The Dataframe will then be written to a file as specified by the output_path.
    The datatframe df_started_runs is joined with the job outputs to get the real ontime.
    '''
    logger.info("Concatenating results from each job and writing result to {}".format(output_path))

    frames = [f for f in job_outputs if isinstance(f, type(pd.DataFrame()))]

    if len(frames) != len(job_outputs):
        logger.warn("Only {} jobs returned a proper DataFrame.".format(len(frames)))

    if len(frames) == 0:
        return

    df_returned_data = []
    df_data = []

    for frame in frames:
        if set(['night', 'run_id']).issubset(frame.columns):
            df_returned_data.append(frame[['night', 'run_id']].copy())
        elif set(['data_path', 'bunch_index']).issubset(frame.columns):
            df_returned_data.append(frame[['data_path', 'bunch_index']].copy())
        frame.columns = rename_columns(frame.columns)
        add_theta_deg_columns(frame)
        if "delta_t" in list(frame.keys()):
            frame["delta_t_seconds"] = frame.delta_t.apply(lambda x: x.total_seconds())
            frame = frame.drop("delta_t", axis=1)
        df_data.append(frame)

    try:
        write_data_to_output_path(pd.concat(df_data), output_path, key='events', mode='w', index=False, **kwargs)
    except OSError:
        from IPython import embed; embed()


    df_returned_data = pd.concat(df_returned_data, ignore_index=True)
    logger.info("There are a total of {} events in the result".format(len(df_returned_data)))

    if len(df_returned_data) == 0:
        logger.info("No events in the result were returned, something must have gone bad, better go fix it.")
        return

    df_returned_data.drop_duplicates(inplace=True)

    logger.info("Number of started runs {}".format(len(df_started_runs)))

    merge_columns_data = ["night", "run_id", "ontime", "delta_t", ]

    if df_started_runs is not None:
        df_started_reduced = df_started_runs
        try:
            if (set(['night','run_id']).issubset(df_started_runs.columns)
                    and set(['night','run_id']).issubset(df_returned_data.columns)):
                df_started_reduced = df_started_runs[merge_columns_data]
                df_merged = pd.merge(df_started_reduced, df_returned_data['night','run_id'], on=['night','run_id'], how='outer', indicator=True)
            elif (set(['data_path','bunch_index']).issubset(df_started_runs.columns)
                  and set(['data_path','bunch_index']).issubset(df_returned_data.columns)):
                df_merged = pd.merge(df_started_reduced, df_returned_data['data_path','bunch_index'], on=['data_path','bunch_index'], how='outer', indicator=True)
            else:
                df_merged = df_started_runs
                df_merged["_merge"] = "both"
        except MemoryError:
            from IPython import embed; embed()

        df_merged["failed"] = (df_merged["_merge"] != "both")
        df_merged.drop("_merge", axis=1, inplace=True)

        df_successfull = df_merged.query("failed == False")
        df_failed = df_merged.query("failed == True")

        if 'ontime' in df_successfull.columns:
            total_on_time_in_seconds = df_successfull.ontime.sum()
            logger.info("Effective on time: {}. Thats {} hours.".format(datetime.timedelta(seconds=total_on_time_in_seconds), total_on_time_in_seconds/3600))

            df_returned_data["total_on_time_in_seconds"] = total_on_time_in_seconds

        logger.info("Number of failed runs: {}".format(len(df_failed)))
        if len(df_failed) > 0:
            name, extension = os.path.splitext(output_path)
            failed_file_list_path = name+"_failed_runs.csv"

            logger.info("Writing list of failed runs to: {}".format(failed_file_list_path))
            key_list = list(df_started_reduced.columns)
            if "PBS_JOBID" in df_failed.columns:
                key_list.append("PBS_JOBID")
            df_failed.to_csv(failed_file_list_path, columns=key_list, **kwargs)

    if (df_started_runs is not None) and (len(df_started_runs) > 0):
        if "delta_t" in list(df_started_runs.keys()):
            df_started_runs["delta_t_seconds"] = df_started_runs.delta_t.apply(lambda x: x.total_seconds())
            df_started_runs = df_started_runs.drop("delta_t", axis=1)
        write_data_to_output_path(df_started_runs, output_path, key='runs', mode='a', **kwargs)




def write_data_to_output_path(df_returned_data, output_path, key='data', mode='w', **kwargs):
    name, extension = os.path.splitext(output_path)
    if extension not in ['.json', '.h5', '.hdf5', '.hdf' , '.csv']:
        logger.warn("Did not recognize file extension {}. Writing to JSON".format(extension))
        df_returned_data.to_json(output_path, orient='records', date_format='epoch', **kwargs )
    elif extension == '.json':
        logger.info("Writing JSON to {}".format(output_path))
        df_returned_data.to_json(output_path, orient='records', date_format='epoch', **kwargs )
    elif extension in ['.h5', '.hdf','.hdf5']:
        logger.info("Writing HDF5 group {} to {}, mode={}".format(key, output_path, mode))
        write_data(df_returned_data, output_path, key=key, mode=mode, **kwargs)
    elif extension == '.csv':
        logger.info("Writing CSV to {}".format(output_path))
        df_returned_data.to_csv(output_path, **kwargs)


def load(
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
        columns=list(default_columns)+['fEffectiveOn'],
    )

    # now lets get all drs runs
    drs_conditions = [
        'fNight >= {}'.format(earliest_night),
        'fNight <= {}'.format(latest_night),
    ]

    drs_data = get_drs_runs(
        factdb, conditions=drs_conditions,
        columns=('fNight AS night', 'fRunID AS run_id', 'fRunStart AS run_start', 'fRunStop AS run_stop'),
    )

    if len(data) == 0 or len(drs_data) == 0:
        logger.error('No data or drs files found that adhere to the specified query.')
        return None

    logger.info("Got {} data runs and {} drs runs".format(len(data), len(drs_data)))

    # the timestamp should be unique for each observation.
    # No two observations start at the same time
    data.set_index("run_start", inplace=True)
    drs_data.set_index("run_start", inplace=True)
    # sorting data by their timestamp.
    data = data.sort_index()
    drs_data = drs_data.sort_index()

    # write filenames
    data["filename"] = build_filename(data.night, data.run_id)
    drs_data["filename"] = build_filename(drs_data.night, drs_data.run_id)

    # write path
    data["path"] = data.apply(build_path, axis=1, path_to_data=path_to_data, extension='.fits.fz')
    drs_data["path"] = drs_data.apply(build_path, axis=1, path_to_data=path_to_data, extension='.drs.fits.gz')

    #remove all none existing drs files
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
    earlier_drs_entries["deltaT"] = np.abs(earlier_drs_entries.run_stop - data.run_stop)
    later_drs_entries["deltaT"] = np.abs(later_drs_entries.run_stop - data.run_stop).fillna(axis='index', method='ffill')
    d_later = later_drs_entries[later_drs_entries.deltaT < earlier_drs_entries.deltaT]
    d_early = earlier_drs_entries[later_drs_entries.deltaT >= earlier_drs_entries.deltaT]

    closest_drs_entries = pd.concat([d_early, d_later])
    closest_drs_entries = closest_drs_entries[closest_drs_entries.deltaT < timedelta(minutes = timedelta_in_minutes)]

    mapping = pd.concat([
        closest_drs_entries.filename,
        closest_drs_entries.path,
        data.path,
        closest_drs_entries.deltaT,
        data.ontime, data.fEffectiveOn,
        data.night,
        data.run_id,
        data.zenith,
        data.azimuth,
        data.right_ascension,
        data.declination,
        data.run_stop,
        data.source,
    ], axis=1, keys=[
        "filename",
        "drs_path",
        "data_path",
        "delta_t",
        "ontime",
        "effective_on",
        "night",
        "run_id",
        "zenith",
        "azimuth",
        "right_ascension",
        "declination",
        "run_stop",
        "source",
    ])

    mapping = mapping.dropna(how='any')

    logger.info("Fetched {} data runs and approx {} drs entries from database where time delta is less than {} minutes".format(len(mapping), mapping['drs_path'].nunique(), timedelta_in_minutes))
    # effective_ontime = (mapping['ontime'] * mapping['effective_on']).sum()
    # logger.info("Effective on time: {}. Thats {} hours.".format(datetime.timedelta(seconds=effective_ontime), effective_ontime/3600))

    return mapping


def ft_json_to_df(json_path):
    with open(json_path,'r') as text:
        try:
            logger.info("Reading fact-tools output.")
            y=json.loads(text.read())
            df_out=pd.DataFrame(y)
            try:
                df_out["PBS_JOBID"]=os.environ['PBS_JOBID']
            except:
                logger.info("PBS_JOBID not in env")
            logger.info("Returning data frame with {} entries".format(len(df_out)))
            return df_out
        except ValueError:
            logger.exception("Fact-tools output could not be read.")
            return "error reading json"
        except Exception:
            logger.exception("Fact-tools output could not be gathered.")
            return "error gathering output"
