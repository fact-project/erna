import peewee
import logging
import os

from .database import(
    RawDataFile, DrsFile, Job,
    ProcessingState, Jar, XML
)


log = logging.getLogger(__name__)

__all__ = [
    'fill_data_runs', 'fill_drs_runs', 'find_drs_file',
    'count_jobs', 'insert_new_job'
]


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


def get_pending_jobs(database):
    runs = (
        Job
        .select()
        .where(ProcessingState.description == 'inserted')
        .order_by(
            RawDataFile.night.desc(),
            Job.priority,
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
        raw_data_file,
        jar,
        xml,
        priority=5,
        location=None,
        closest_drs_file=True,
        ):

    xml_version = Jar.select(Jar.version).where(Jar.id == xml.jar_id).get().version
    if not xml_version == jar.version:
        raise ValueError('FACT Tools versions of xml does not fit requested version')

    drs_file = find_drs_file(
        raw_data_file,
        location=location,
        closest=closest_drs_file,
    )

    job = Job(
        raw_data_file=raw_data_file,
        drs_file=drs_file,
        jar=jar,
        status=ProcessingState.get(description='inserted'),
        priority=priority,
        xml=xml,
    )

    job.save()


def count_jobs(state=None):
    query = Job.select()

    if state is not None:
        query = query.join(ProcessingState)
        query = query.where(ProcessingState.description == state)

    return query.count()


def save_xml(xml_id, data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    xml = XML.select(XML.name, XML.jar_id).get()
    version = Jar.select(Jar.version).where(Jar.id == xml.jar_id).get().version

    xml_dir = os.path.join(data_dir, 'xmls')
    xml_file = os.path.join(xml_dir, '{}-{}.xml'.format(
        xml.name,
        version
    ))
    if not os.path.isfile(xml_file):
        xml = XML.get()
        os.makedirs(xml_dir, exist_ok=True)

        with open(xml_file, 'w') as f:
            f.write(xml.content)

    return xml_file


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


def build_output_base_name(job):
    version = Jar.select(Jar.version).where(Jar.id == job.jar_id).get().version
    return '{night:%Y%m%d}_{run_id:03d}_{version}_{name}'.format(
        night=job.raw_data_file.night,
        run_id=job.raw_data_file.run_id,
        version=version,
        name=job.xml.name
    )
