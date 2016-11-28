import logging

from .database import RawDataFile, DrsFile, FACTToolsRun, ProcessingState, RawDataFile


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
