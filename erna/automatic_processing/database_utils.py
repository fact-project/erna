import peewee
import logging

from .database import RawDataFile, DrsFile, FACTToolsRun, ProcessingState


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


def find_drs_file(raw_data_file, location=None, closest=True):
    '''
    Find a drs file for the give raw data file.
    If closest is True, return the nearest (in terms of runid) drs file,
    else return the drs run just before the data run.

    If location is not None, only drs files which are available at the
    given location are taken into account.
    '''
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


def insert_new_job(
        night,
        run_id,
        fact_tools_version,
        xml,
        priority=5,
        location=None,
        closest_drs_file=True,
        ):

    if not xml.fact_tools_version.version == fact_tools_version.version:
        raise ValueError('FACT Tools versions of xml does not fit requested version')

    raw_data_file = RawDataFile.get(night=night, run_id=run_id)

    drs_file = find_drs_file(
        raw_data_file,
        location=location,
        closest=closest_drs_file,
    )

    fact_tools_run = FACTToolsRun(
        raw_data_file=raw_data_file,
        drs_file=drs_file,
        fact_tools_version=fact_tools_version,
        status=ProcessingState.get(description='inserted'),
        priority=priority,
        xml=xml,
    )

    fact_tools_run.save()
