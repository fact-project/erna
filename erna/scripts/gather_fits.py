import pandas as pd
import click
import h5py
import dateutil.parser
import sys
import os
import numpy as np
from fact.io import to_h5py

from ..automatic_processing.database import (
    database, Job, RawDataFile, Jar, XML, ProcessingState
)
from ..utils import load_config, create_mysql_engine
from ..hdf_utils import write_fits_to_hdf5
from ..datacheck import get_runs
from ..datacheck_conditions import conditions as datacheck_conditions


@click.command()
@click.argument('xml-name')
@click.argument('ft-version')
@click.argument('outputfile')
@click.option(
    '--config', '-c', type=click.Path(exists=True, dir_okay=False),
    help='Path to yaml config file with credentials'
)
@click.option('--start', '-s', help='First night to get data from')
@click.option('--end', '-e', help='Last night to get data from')
@click.option('--source', default='Crab')
@click.option('--datacheck', help='The name of a condition set for the datacheck')
@click.option('--runlist', help='A csv file with columns night, run_id, the runs to get')
@click.option('-r', '--run-type', default='data', help='The runtype to consider')
def main(xml_name, ft_version, outputfile, config, start, end, source, datacheck, runlist, run_type):
    '''
    Gather the fits outputfiles of the erna automatic processing into a hdf5 file.
    The hdf5 file is written using h5py and contains the level 2 features in the
    `events` group and some metadata for each run in the `runs` group.

    It is possible to only gather files that pass a given datacheck with the --datacheck
    option. The possible conditions are implemented in erna.datacheck_conditions/

    XML_NAME: name of the xml for which you want to gather output
    FT_VERSION: FACT Tools version for which you want to gather output
    OUTPUTFILE: the outputfile
    '''
    config = load_config(config)
    database.init(**config['processing_database'])
    database.connect()

    if datacheck and runlist:
        print('Only one of datacheck or runlist allowed')
        sys.exit(1)

    if datacheck is not None:
        if not (datacheck in datacheck_conditions or os.path.isfile(datacheck)):
            print('Conditions must be a file or any of: ')
            for key in datacheck_conditions:
                print(key)
            sys.exit(1)

    processing_db = create_mysql_engine(**config['processing_database'])
    fact_db = create_mysql_engine(**config['fact_database'])

    try:
        jar = (
            Jar
            .select(Jar.id, Jar.version)
            .where(Jar.version == ft_version)
            .get()
        )
    except Jar.DoesNotExist:
        print('FACT-Tools version not found, avaliable jars are')
        for jar in Jar.select(Jar.version):
            print(jar.version)
        sys.exit(1)

    try:
        xml = XML.get(jar=jar, name=xml_name)
    except XML.DoesNotExist:
        print('XML not found, avaliable xmls are:')
        for xml in XML.select(XML.name).join(Jar).where(Jar.version == ft_version):
            print(xml.name)
        sys.exit(1)

    job_query = (
        Job
        .select(
            RawDataFile.night.alias('night'), RawDataFile.run_id.alias('run_id'),
            Job.result_file, ProcessingState.description.alias('status')
        )
        .join(RawDataFile)
        .switch(Job)
        .join(ProcessingState)
        .where(
            Job.jar == jar,
            Job.xml == xml,
            RawDataFile.run_type_name == run_type,
        )
    )
    if start:
        start = dateutil.parser.parse(start).date()
        job_query = job_query.where(RawDataFile.night >= start)
    if end:
        end = dateutil.parser.parse(end).date()
        job_query = job_query.where(RawDataFile.night <= end)

    sql, params = job_query.sql()

    with processing_db.connect() as conn:
        jobs = pd.read_sql_query(sql, conn, params=params)
    if runlist is None:
        conditions = [
            'fNight <= {}'.format(jobs.night.max()),
            'fNight >= {}'.format(jobs.night.min()),
            'fSourceName = "{}"'.format(source),
        ]
    else:
        wanted_runs = pd.read_csv(runlist)
        conditions = [
            'fNight <= {}'.format(wanted_runs.night.max()),
            'fNight >= {}'.format(wanted_runs.night.min()),
        ]

    if datacheck is not None:
        if os.path.isfile(datacheck):
            with open(datacheck, 'r') as f:
                conditions.extend(f.read().splitlines())
        else:
            conditions.extend(datacheck_conditions[datacheck])

    runs = get_runs(fact_db, conditions=conditions).set_index(['night', 'run_id'])
    jobs = jobs.join(runs, on=['night', 'run_id'], how='inner')

    if runlist is not None:
        jobs = wanted_runs.join(
            jobs.set_index(['night', 'run_id']),
            on=['night', 'run_id'],
            how='inner',
            lsuffix='user_input_',
        )

    successful_jobs = jobs.query('status == "success"')
    total = len(jobs)
    successful = len(successful_jobs)

    if runlist is not None:
        if len(wanted_runs) != len(jobs):
            click.confirm(
                'Only {} of {} runs available, continue?:'.format(
                    total, len(jobs)
                ),
                abort=True,
            )

    if total != successful:
        click.confirm(
            'Only {} of {} jobs successful, continue?'.format(successful, total),
            abort=True,
        )

    print('Found {} runs with a total ontime of {:1.2f} h'.format(
        len(jobs), jobs.ontime.sum()/3600
    ))

    if os.path.isfile(outputfile):
        a = input('Outputfile exists! Overwrite? [y, N]: ')
        if not a.lower().startswith('y'):
            sys.exit()

    columns = [
        'night',
        'run_id',
        'source',
        'ontime',
        'right_ascension',
        'declination',
        'zenith',
        'azimuth',
        'run_start',
        'run_stop',
    ]
    to_h5py(successful_jobs[columns], outputfile, key='runs', mode='w')

    with h5py.File(outputfile, 'a') as f:
        if runlist is not None:
            f['runs'].attrs['datacheck'] = 'RUNLIST'
        else:
            f['runs'].attrs['datacheck'] = ' AND '.join(conditions)

    write_fits_to_hdf5(outputfile, successful_jobs.result_file, mode='a')
