import logging

from .database import RawDataFile, DrsFile, FACTToolsRun, ProcessingState
import peewee


log = logging.getLogger(__name__)


def fill_data_runs(df, database):
    df = df.copy()
    df.rename(columns={'fNight': 'night', 'fRunID': 'run_id'}, inplace=True)
    df.drop(['fDrsStep', 'fRunTypeKey'], axis=1, inplace=True)
    with database.atomic():
        RawDataFile.insert_many(df.to_dict(orient='records')).upsert().execute()


def fill_drs_runs(df, database):
    df = df.copy()
    df.rename(columns={'fNight': 'night', 'fRunID': 'run_id'}, inplace=True)
    df.drop(['fDrsStep', 'fRunTypeKey'], axis=1, inplace=True)
    with database.atomic():
        DrsFile.insert_many(df.to_dict(orient='records')).upsert().execute()


def get_pending_fact_tools_runs(database):
    runs = (
        FACTToolsRun
        .select()
        .where(ProcessingState.description == 'inserted')
        .order_by(
            RawDataFile.night.desc(),
            FACTToolsRun.priority,
        )
    )
    return runs


def find_closest_drs_file(raw_data_file, location=None, closest=True):
    query = DrsFile.select()
    query = query.where(DrsFile.night == raw_data_file.night)
    if location is not None:
        if location == 'isdc':
            query = query.where(DrsFile.available_isdc)
        elif location == 'dortmund':
            query = query.where(DrsFile.available_dortmund)
        else:
            raise ValueError('Unknown location {}'.format(location))

    if closest is True:
        query = query.order_by(peewee.fn.Abs(DrsFile.run_id - raw_data_file.run_id))
    else:
        query = (query
                 .where(DrsFile.run_id < raw_data_file.run_id)
                 .order_by(DrsFile.run_id.desc()))
    try:
        drs_file = query.get()
    except DrsFile.DoesNotExist:
        raise ValueError('No DrsFile found for {:%Y%m%d}_{:03d}'.format(
            raw_data_file.night, raw_data_file.run_id
        ))

    return drs_file
