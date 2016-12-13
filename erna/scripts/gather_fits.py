import pandas as pd
from astropy.io import fits
import click
import h5py
import dateutil.parser
import sys

from ..automatic_processing.database import (
    setup_database, database, Job, RawDataFile, Jar, XML, ProcessingState
)
from ..utils import load_config, create_mysql_engine, night_int_to_date
from ..hdf_utils import write_fits_to_hdf5, append_to_hdf5, initialize_hdf5
from ..datacheck import get_runs



@click.command()
@click.argument('xml-name')
@click.argument('ft-version')
@click.argument('outputfile')
@click.option('--config', '-c')
@click.option('--start', '-s')
@click.option('--end', '-e', )
@click.option('--source', default='Crab')
def main(xml_name, ft_version, outputfile, config, start, end, source):
    config = load_config(config)
    database.init(**config['processing_database'])
    database.connect()

    processing_db = create_mysql_engine(**config['processing_database'])
    fact_db = create_mysql_engine(**config['fact_database'])

    jar = (
        Jar
        .select(Jar.id, Jar.version)
        .where(Jar.version == ft_version)
        .get()
    )

    xml = XML.get(jar=jar, name=xml_name)

    job_query = (
        Job
        .select(
            RawDataFile.night, RawDataFile.run_id,
            Job.result_file, ProcessingState.description.alias('status')
        )
        .join(RawDataFile)
        .switch(Job)
        .join(ProcessingState)
        .where(Job.jar == jar, Job.xml == xml)
    )
    if start:
        start = dateutil.parser.parse(start).date()
        job_query = job_query.where(RawDataFile.night >= start)
    if end:
        end = dateutil.parser.parse(end).date()
        job_query = job_query.where(RawDataFile.night <= end)

    sql, params = job_query.sql()

    jobs = pd.read_sql_query(sql, processing_db, params=params)
    runs = get_runs(fact_db, conditions=[
        'fRunTypeName = "data"',
        'fNight <= {}'.format(jobs.night.max()),
        'fNight >= {}'.format(jobs.night.min()),
        'fSourceName = "{}"'.format(source),
    ])
    jobs = jobs.join(runs, on=['night', 'run_id'], how='inner')
    successful_jobs = jobs.query('status == "success"')

    total = len(jobs)
    successful = len(successful_jobs)
    if total != successful:
        answer = input('Only {} of {} jobs finished, continue? [y, N] :'.format(
            successful, total
        ))
        if not answer.lower().startswith('y'):
            sys.exit()

    print('Found {} runs with a total ontime of {:1.2f} h'.format(
        len(jobs), jobs.ontime.sum()/3600
    ))

    cols = ['night', 'run_id', 'source', 'ontime']
    runs_array = successful_jobs[cols].to_records(index=False)
    initialize_hdf5(outputfile, dtypes=runs_array.dtype, groupname='runs')
    append_to_hdf5(outputfile, runs_array, groupname='runs')

    write_fits_to_hdf5(outputfile, successful_jobs.result_file)
