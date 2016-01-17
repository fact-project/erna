import logging
import pandas as pd
import os
import numpy as np
import datetime
from datetime import timedelta
# from . import stream_runner

logger = logging.getLogger(__name__)

def collect_output(job_outputs, output_path):
    '''
    Collects the output from the list of job_outputs and merges them into a dataframe. The Dataframe will then be written
    to a file as specified by the output_path.
    '''
    logger.info("Concatenating results from each job and writing result to {}".format(output_path))
    frames = [f for f in job_outputs if isinstance(f, type(pd.DataFrame()))]
    if len(frames) != len(job_outputs):
        logger.warn("Only {} jobs returned a proper DataFrame.".format(len(frames)))

    if len(frames) == 0:
        return

    df = pd.concat(frames, ignore_index=True)
    logger.info("There are a total of {} events in the result".format(len(df)))

    name, extension = os.path.splitext(output_path)
    if extension == 'json':
        logger.info("Writing JSON to {}".format(output_path))
        df.to_json(output_path, orient='records', date_format='epoch' )
    elif extension == 'h5' or extension == 'hdf' or extension == 'hdf5':
        logger.info("Writing HDF5 to {}".format(output_path))
        df.to_hdf(output_path, 'table', mode='w')
    elif extension == 'csv':
        logger.info("Writing CSV to {}".format(output_path))
        df.to_csv(output_path)
    else:
        logger.warn("Did not recognize file extension {}. Writing to JSON".format(extension))
        df.to_json(output_path, orient='records', date_format='epoch' )


def load(earliest_night, latest_night, path_to_data, factdb,source_name="Crab", timedelta_in_minutes="30"):
    '''
    Given the earliest and latest night to fetch as a factnight string (as in 20141024) this method returns a DataFrame
    containing the paths to data files and their correpsonding .drs files. The maximum time difference between the
    data and drs files is specified by the timedelta_in_minutes parameter.
    '''

    def build_path(fNight, path_to_data):
            year = fNight[0:4]
            month = fNight[4:6]
            day = fNight[6:8]
            return os.path.join(path_to_data, year, month, day)

    def build_RunID(fRunID):
        if(len(fRunID) == 1):
            return('00' + fRunID)

        if(len(fRunID) == 2):
            return('0' + fRunID)

        return(fRunID)

    logger.debug("Table names in DB: ")
    logger.debug(factdb.table_names())

    if (len(factdb.table_names()) > 0):
        logger.info("Connected to Database.")


    #read source table
    sourceDB = pd.read_sql_table('Source', factdb, columns=["fSourceName", "fSourceKEY"])

    firstNight = earliest_night
    lastNight = latest_night


    columns  = ["fNight", "fROI", "fRunTypeKey", "fRunStart", "fRunStop", "fSourceKEY",
                "fCurrentsDevMean", "fMoonZenithDistance", "fThresholdMedian", "fEffectiveOn", "fCurrentsMedMean",
                "fZenithDistanceMean", "fDrsStep", "fRunID", "fOnTime", "fHasDrsFile", "fTriggerRateMedian",
                "fThresholdMinSet"]


    logger.info("Reading Data from DataBase from " + str(firstNight) + " to " + str(lastNight) +
                " for source: " + source_name)

    rundb = pd.read_sql("SELECT " + ",".join(columns) + " from RunInfo WHERE (fNight > " + firstNight + " AND fNight <" + lastNight + ") ", factdb)


    #lets try to get all data runs from a specific source between two dates
    source = sourceDB[sourceDB.fSourceName == source_name]
    if source.size != 2:
        # embed()
        logger.error('Given source name does not return a specific source from the DB')
        logger.error('Allowed names are: {}'.format(sourceDB.fSourceName) )
        return pd.DataFrame()

    #define conditions for data quality
    conditions = [
        "fRunTypeKey == 1", # Data Events
        "fMoonZenithDistance > 100",
        #"fCurrentsMedMean < 7", # low light conditions hopefully
         "fROI == 300",
         "fZenithDistanceMean < 30",
         "fSourceKEY == " + str(int(source.fSourceKEY)),
         "fTriggerRateMedian > 40",
         "fTriggerRateMedian < 85",
         "fOnTime > 0.95",
         "fThresholdMinSet < 350"
    ]
    querystring = " & ".join(conditions)

    #get all data runs according to the conditions
    data = rundb.query(querystring)



    # now lets get all drs runs
    conditions = [
        "fRunTypeKey == 2", #  300 roi Drs files
        "fROI == 300",
        "fDrsStep == 2",
        # "fZenithDistanceMean < 30",
        # "fSourceKEY == " + str(int(source.fSourceKEY)),
    ]

    querystring = " & ".join(conditions)
    drs_data = rundb.query(querystring)

    logger.info("Got " + str(len(data)) + " data entries and " + str(len(drs_data)) + " drs entries")

    #the timestamp should be unique for each observation. No two observations start at the same time
    data = data.set_index("fRunStart")
    drs_data = drs_data.set_index("fRunStart")



    #write filenames
    data["filename"] = data.fNight.astype(str) + "_" + data.fRunID.astype(str).apply(build_RunID)
    drs_data["filename"] = drs_data.fNight.astype(str) + "_" + drs_data.fRunID.astype(str).apply(build_RunID)

    #write path TODO: file ending? is everythiong in fz?
    data["path"] = data.fNight.astype(str).apply(build_path, args=[path_to_data]) + "/" + data.filename.astype(str) + ".fits.fz"
    drs_data["path"] = drs_data.fNight.astype(str).apply(build_path, args=[path_to_data]) + "/" + drs_data.filename.astype(str) + ".drs.fits.gz"


    #sorting data by their timestamp.
    data = data.sort_index()
    drs_data = drs_data.sort_index()

    #reindex the drs table using the index of the data table. There are always more data runs than drs run in the db.
    #hence missing rows have to be filled either forward or backwards
    earlier_drs_entries = drs_data.reindex(data.index, method="ffill")
    later_drs_entries = drs_data.reindex(data.index, method="backfill")


    #when backfilling the drs obeservations the last rows might be invalid and contain nans. We cannot drop them becasue the tables have to have the same length.
    # in that case simply fill them up.
    earlier_drs_entries["deltaT"] = np.abs(earlier_drs_entries.fRunStop - data.fRunStop)
    later_drs_entries["deltaT"] = np.abs(later_drs_entries.fRunStop - data.fRunStop).fillna(axis='index', method='ffill')
    d_later = later_drs_entries[later_drs_entries.deltaT < earlier_drs_entries.deltaT]
    d_early = earlier_drs_entries[later_drs_entries.deltaT >= earlier_drs_entries.deltaT]

    closest_drs_entries = pd.concat([d_early, d_later])
    closest_drs_entries = closest_drs_entries[closest_drs_entries.deltaT < timedelta(minutes = timedelta_in_minutes)]


    mapping = pd.concat([closest_drs_entries.filename, closest_drs_entries.path, data.path, closest_drs_entries.deltaT, data.fOnTime, data.fEffectiveOn], axis=1, keys=["filename", "drs_path", "data_path", "delta_t", "on_time", "effective_on"])
    mapping = mapping.dropna( how='any')

    logger.info("Fetched {} data runs and approx {} drs entries from database where time delta is less than {} minutes".format(len(mapping), mapping['drs_path'].nunique(), timedelta_in_minutes))
    effective_on_time = (mapping['on_time'] * mapping['effective_on']).sum()
    logger.info("Effective on time: {}. Thats {} hours.".format(datetime.timedelta(seconds=effective_on_time), effective_on_time/3600))

    return mapping
