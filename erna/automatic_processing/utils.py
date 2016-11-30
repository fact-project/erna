import re
from datetime import date
import os


datafile_re = re.compile(r'(?:.*/)?([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{3})\.fits(?:\.[fg]z)?$')
drsfile_re = re.compile(r'(?:.*/)?([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{3})\.drs\.fits(?:\.gz)?$')


def parse_path(path):
    match = datafile_re.search(path)
    if match is None:
        match = drsfile_re.search(path)
    if match is None:
        raise IOError('File seems not to be a drs or data file')

    year, month, day, run_id = map(int, match.groups())

    return date(year, month, day), run_id


def save_xml(xml, data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    xml_dir = os.path.join(data_dir, 'xmls')
    xml_file = os.path.join(xml_dir, '{}-{}.xml'.format(
        xml.name,
        xml.fact_tools_version.version
    ))
    if not os.path.isfile(xml_file):
        os.makedirs(xml_dir, exist_ok=True)

        with open(xml_file, 'w') as f:
            f.write(xml.content)

    return xml_file


def save_jar(fact_tools_version, data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    jar_dir = os.path.join(data_dir, 'jars')
    jar_file = os.path.join(
        jar_dir,
        'fact-tools-{}.jar'.format(fact_tools_version.version)
    )
    if not os.path.isfile(jar_file):
        os.makedirs(jar_dir, exist_ok=True)

        with open(jar_file, 'wb') as f:
            f.write(fact_tools_version.jar_file)

    return jar_file


def get_aux_dir(night, location='isdc'):

    if location == 'dortmund':
        basepath = '/fhgfs/groups/app/fact/aux'
    else:
        basepath = '/fact/aux'

    return os.path.join(
        basepath,
        '{:04d}'.format(night.year),
        '{:02d}'.format(night.month),
        '{:02d}'.format(night.day)
    )


def build_output_directory_name(fact_tools_job, output_base_dir):
    return os.path.join(
        output_base_dir,
        fact_tools_job.fact_tools_version.version,
        fact_tools_job.xml.name,
        '{:04d}'.format(fact_tools_job.raw_data_file.night.year),
        '{:02d}'.format(fact_tools_job.raw_data_file.night.month),
        '{:02d}'.format(fact_tools_job.raw_data_file.night.day)
    )


def build_output_base_name(fact_tools_job):
    return '{night:%Y%m%d}_{run_id:03d}_{version}_{name}'.format(
        night=fact_tools_job.raw_data_file.night,
        run_id=fact_tools_job.raw_data_file.run_id,
        version=fact_tools_job.fact_tools_version.version,
        name=fact_tools_job.xml.name
    )
