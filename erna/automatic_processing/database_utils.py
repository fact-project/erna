import peewee
import logging
import os
from tqdm import tqdm

from .database import (
    RawDataFile, DrsFile, Job,
    ProcessingState, Jar, XML,
    database
)


log = logging.getLogger(__name__)

__all__ = [
    'fill_data_runs', 'fill_drs_runs', 'find_drs_file',
    'count_jobs', 'insert_new_job', 'insert_new_jobs',
    'resubmit_walltime_exceeded', 'update_job_status',
]


@database.connection_context()
def update_job_status(job, description):
    status = ProcessingState.select().where(ProcessingState.description == description)
    return Job.update(status=status).where(Job.id == job.id).execute()


@database.connection_context()
def fill_data_runs(df, database):
    if len(df) == 0:
        return
    df = df.copy()
    df.rename(
        columns={
            'fNight': 'night',
            'fRunID': 'run_id',
            'fRunTypeKey': 'run_type_key',
            'fRunTypeName': 'run_type_name',
            'fROI': 'roi',
        },
        inplace=True,
    )
    with database.atomic():
        query = (
            RawDataFile
            .insert_many(df.to_dict(orient='records'))
            .on_conflict('IGNORE')
        )
        sql, params = query.sql()
        # See https://github.com/coleifer/peewee/issues/1067
        sql = sql.replace('INSERT OR IGNORE', 'INSERT IGNORE')
        database.execute_sql(sql, params=params)


@database.connection_context()
def fill_drs_runs(df, database):
    if len(df) == 0:
        return
    df = df.copy()
    df.drop(['fRunTypeKey', 'fRunTypeName'], axis=1, inplace=True)
    df.rename(
        columns={
            'fNight': 'night',
            'fRunID': 'run_id',
            'fROI': 'roi',
            'fDrsStep': 'drs_step',
        },
        inplace=True,
    )
    with database.atomic():
        query = (
            DrsFile
            .insert_many(df.to_dict(orient='records'))
            .on_conflict('IGNORE')
        )
        sql, params = query.sql()
        # See https://github.com/coleifer/peewee/issues/1067
        sql = sql.replace('INSERT OR IGNORE', 'INSERT IGNORE')
        database.execute_sql(sql, params=params)


@database.connection_context()
def get_pending_jobs(limit=None):
    runs = (
        Job
        .select()
        .join(ProcessingState)
        .switch(Job)
        .join(RawDataFile)
        .where(ProcessingState.description == 'inserted')
        .order_by(Job.priority, RawDataFile.night.desc())
    )
    if limit is not None:
        runs = runs.limit(limit)
    return runs


@database.connection_context()
def find_drs_file(raw_data_file, closest=True):
    '''
    Find a drs file for the give raw data file.
    If closest is True, return the nearest (in terms of runid) drs file,
    else return the drs run just before the data run.
    '''
    query = DrsFile.select()
    query = query.where(DrsFile.night == raw_data_file.night)

    if raw_data_file.roi == 300:
        query = query.where((DrsFile.drs_step == 2) & (DrsFile.roi == 300))
    elif raw_data_file.roi == 1024:
        query = query.where((DrsFile.drs_step == 1) & (DrsFile.roi == 1024))
    else:
        raise ValueError('ROI {} not supported'.format(raw_data_file.roi))

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


@database.connection_context()
def insert_new_job(
        raw_data_file,
        jar,
        xml,
        walltime,
        priority=5,
        closest_drs_file=True,
        ):
    '''
    Insert a new job into the database

    Parameters
    ----------
    raw_data_file: RawDataFile
        the input file for the job
    jar: Jar
        the fact-tools jar to use
    xml: XML
        the xml to use
    walltime: walltime
        the walltime to use
    priority: int
        Priority for the Job. Lower numbers mean more important.
    closest_drs_file: bool
        If true, take the closest drs file else take the closest before the data run
    '''

    xml_version = Jar.select(Jar.version).where(Jar.id == xml.jar_id).get().version
    if not xml_version == jar.version:
        raise ValueError(
            'FACT Tools versions of xml ({}) does not fit jar version ({})'.format(
                xml_version, jar.version
            )
        )

    drs_file = find_drs_file(
        raw_data_file,
        closest=closest_drs_file,
    )

    job = Job(
        raw_data_file=raw_data_file,
        drs_file=drs_file,
        jar=jar,
        walltime=walltime,
        status=ProcessingState.get(description='inserted'),
        priority=priority,
        xml=xml,
    )

    job.save()


@database.connection_context()
def insert_new_jobs(raw_data_files, jar, xml, walltime, progress=True, **kwargs):

    if isinstance(raw_data_files, list):
        total = len(raw_data_files)
    else:
        total = raw_data_files.count()

    failed_files = []
    for f in tqdm(raw_data_files, total=total, disable=not progress):
        try:
            insert_new_job(f, jar=jar, xml=xml, walltime=walltime, **kwargs)
        except peewee.IntegrityError:
            log.warning('Job already submitted: {}_{:03d}'.format(f.night, f.run_id))
        except ValueError as e:
            log.warning('Could not submit {}_{:03d}: {}'.format(
                f.night, f.run_id, e,
            ))
            failed_files.append(f)
    return failed_files


@database.connection_context()
def count_jobs(state=None):
    query = Job.select()

    if state is not None:
        query = query.join(ProcessingState)
        query = query.where(ProcessingState.description == state)

    return query.count()


@database.connection_context()
def save_xml(xml_id, data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    xml = XML.select(XML.name, XML.jar_id).where(XML.id == xml_id).get()
    version = Jar.select(Jar.version).where(Jar.id == xml.jar_id).get().version

    xml_dir = os.path.join(data_dir, 'xmls')
    xml_file = os.path.join(xml_dir, '{}-{}.xml'.format(
        xml.name,
        version
    ))
    if not os.path.isfile(xml_file):
        xml = XML.get(id=xml_id)
        os.makedirs(xml_dir, exist_ok=True)

        with open(xml_file, 'w') as f:
            f.write(xml.content)

    return xml_file


@database.connection_context()
def save_jar(jar_id, data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    jar = Jar.select(Jar.version).where(Jar.id == jar_id).get()

    jar_dir = os.path.join(data_dir, 'jars')
    jar_file = os.path.join(
        jar_dir,
        'fact-tools-{}.jar'.format(jar.version)
    )
    if not os.path.isfile(jar_file):
        os.makedirs(jar_dir, exist_ok=True)

        jar = Jar.get(id=jar_id)
        with open(jar_file, 'wb') as f:
            f.write(jar.data)

    return jar_file


@database.connection_context()
def build_output_directory_name(job, output_base_dir):
    version = Jar.select(Jar.version).where(Jar.id == job.jar_id).get().version
    return os.path.join(
        output_base_dir,
        version,
        job.xml.name,
        '{:04d}'.format(job.raw_data_file.night.year),
        '{:02d}'.format(job.raw_data_file.night.month),
        '{:02d}'.format(job.raw_data_file.night.day)
    )


@database.connection_context()
def build_output_base_name(job):
    version = Jar.select(Jar.version).where(Jar.id == job.jar_id).get().version
    return '{night:%Y%m%d}_{run_id:03d}_{version}_{name}'.format(
        night=job.raw_data_file.night,
        run_id=job.raw_data_file.run_id,
        version=version,
        name=job.xml.name
    )


@database.connection_context()
def resubmit_walltime_exceeded(factor=1.5):
    '''
    Resubmit jobs where walltime was exceeded.
    '''
    return (
        Job
        .update(walltime=factor * Job.walltime, status=ProcessingState.get(description='inserted'))
        .where(Job.status == ProcessingState.get(description='walltime_exceeded'))
    ).execute()
