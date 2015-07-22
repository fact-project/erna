__author__ = 'kai, lena'

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import logging
import datetime
from datetime import timedelta

logger = logging.getLogger(__name__)

factdb = create_engine("mysql+mysqldb://factread:r3adfac!@129.194.168.95/factdata")

def build_path(fNight):
        year = fNight[0:4]
        month = fNight[4:6]
        day = fNight[6:8]

        return("/fact/raw/" + year + "/" + month + "/" + day)

def build_RunID(fRunID):
    if(len(fRunID) < 2):
        return('00' + fRunID)
    elif(len(fRunID) < 3):
        return('0' + fRunID)
    else:
        return(fRunID)

def load(earliest_run, latest_run, source_name="Crab", loglevel=logging.INFO, timedelta_in_minutes="30", files_per_night=1):
    '''

    :param earliest_run: earliest run to fetch in "YYYMMDD" format as a string
    :param latest_run: lates run to fetch in "YYYMMDD" format as a string
    :param source_name: The name of the Astronomical source. Like "Crab" or "Mrk 421"
    :param loglevel:
    :return:

    '''


    logging.basicConfig(level=loglevel)
    logger.debug("Table names in DB: ")
    logger.debug(factdb.table_names())

    if (len(factdb.table_names()) > 0):
        logger.info("Connected to Database.")


    #read source table
    sourceDB = pd.read_sql_table('Source', factdb, columns=["fSourceName", "fSourceKEY"])

    #print(sourceDB)

    #valid time range
    firstNight = earliest_run
    lastNight = latest_run

    # filename = "drs_mapping.json"


    columns  = ["fNight", "fROI", "fRunTypeKey", "fRunStart", "fRunStop", "fSourceKEY",
                "fCurrentsDevMean", "fMoonZenithDistance", "fThresholdMedian", "fEffectiveOn", "fCurrentsMedMean",
                "fZenithDistanceMean", "fDrsStep", "fRunID", "fOnTime", "fHasDrsFile", "fTriggerRateMedian",
                "fThresholdMinSet"]


    logger.info("Reading Data from DataBase from " + str(firstNight) + " to " + str(lastNight) +
                " for source: " + source_name)

    rundb = pd.read_sql("SELECT " + ",".join(columns) + " from RunInfo WHERE (fNight > " + firstNight + " AND fNight <" + lastNight + ") ", factdb)


    #lets try to get all data runs from a specific source between two dates

    source = sourceDB[sourceDB.fSourceName == source_name]


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
    #print(querystring)

    #get all data runs
    data = rundb.query(querystring)
    #print(data)


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
    #reindex and fill to either previous or later observation

    data = data.set_index("fRunStart")
    drs_data = drs_data.set_index("fRunStart")
    #print(drs_data)


    #write filenames
    data["filename"] = data.fNight.astype(str) + "_" + data.fRunID.astype(str).apply(build_RunID)
    drs_data["filename"] = drs_data.fNight.astype(str) + "_" + drs_data.fRunID.astype(str).apply(build_RunID)

    #write path
    data["path"] = data.fNight.astype(str).apply(build_path) + "/" + data.filename.astype(str) + ".fits.fz"
    drs_data["path"] = drs_data.fNight.astype(str).apply(build_path) + "/" + drs_data.filename.astype(str) + ".drs.fits.gz"

    # #------> alles rausschmeißen was Null ist
    # #drs_data.dropna(thresh=1)

    #sorting
    logger.info("Sorting Data from Database")
    data = data.sort()
    drs_data = drs_data.sort()
  

    logger.info("Reindexing")
    #get earlier and later drs observations
    earlier_drs_entries = drs_data.reindex(data.index, method="ffill")
    later_drs_entries = drs_data.reindex(data.index, method="backfill")
   

    #now get the closest observations
    earlier_drs_entries["deltaT"] = np.abs(earlier_drs_entries.fRunStop - data.fRunStop)
    later_drs_entries["deltaT"] = np.abs(later_drs_entries.fRunStop - data.fRunStop).fillna(earlier_drs_entries.deltaT)

    d_later = later_drs_entries[later_drs_entries.deltaT < earlier_drs_entries.deltaT]
    d_early = earlier_drs_entries[later_drs_entries.deltaT >= earlier_drs_entries.deltaT]

    closest_drs_entries = pd.concat([d_early, d_later])
    closest_drs_entries = closest_drs_entries[closest_drs_entries.deltaT < timedelta(minutes = timedelta_in_minutes)]
    #print(closest_drs_entries)


    #write filenames as json file
    # logger.info("Writing data to " + str(filename))
    mapping = pd.concat([closest_drs_entries.fNight, closest_drs_entries.path, data.path],
                        axis=1, keys=["fNight", "drs_path", "data_path"])
    mapping = mapping.set_index(data.filename)
    

    mapping = mapping.dropna( how='any')
    #print(mapping)



    final_map = pd.DataFrame(columns=["filename", "drs_path", "data_path"])
    night = 0
    k = 0
    for i in range(0,len(mapping)):
        temp = mapping.fNight[i]
        if(temp != night):
            night = temp
            df = pd.DataFrame([[mapping.index[i], mapping.drs_path[i], mapping.data_path[i]]], columns=["filename", "drs_path", "data_path"])
            #final_map.append(df)
            final_map = pd.concat([final_map,df])
            k = 1
        elif(k < files_per_night):
            k = k+1
            df = pd.DataFrame([[mapping.index[i], mapping.drs_path[i], mapping.data_path[i]]], columns=["filename", "drs_path", "data_path"])
            final_map = pd.concat([final_map,df])
    final_map = final_map.set_index(final_map.filename)
    final_map = final_map.drop('filename',  axis=1)
    print(final_map)

        

    logger.info("Fetched {} data runs and approx {} drs entries from database".format(len(data), closest_drs_entries['filename'].nunique()))
    effective_on_time = (data.fOnTime * data.fEffectiveOn).sum()
    logger.info("Effective on time: {}. Thats {} hours.".format(datetime.timedelta(seconds=effective_on_time), effective_on_time/3600))

    final_map.to_json(earliest_run + "_" + latest_run + "_" + source_name + ".json", orient='index' )
    print("Total effective onTime: " + str(datetime.timedelta(seconds=effective_on_time)))

    #also write whitelist
    #data.filename.to_json("{}_whitelist.json".format(name), orient='index', date_format='iso')


if __name__ == '__main__':
    load(earliest_run="20141001", latest_run="20141231", source_name="Crab", timedelta_in_minutes=30, files_per_night=1)