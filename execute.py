import logging
import click
import erna
import stream_runner
import numpy as np
import subprocess
import pandas as pd
from IPython import embed
from datetime import datetime
from sqlalchemy import create_engine
from os import path
import os
import json

from gridmap import Job, process_jobs


logger = logging.getLogger(__name__)


def make_jobs(jar, xml, output_directory, df_mapping,  engine, queue, vmem, num_jobs):
    jobs = []
    # create job objects
    split_indices = np.array_split(np.arange(len(df_mapping)), num_jobs)
    for num, indices in enumerate(split_indices):
        df = df_mapping[indices.min(): indices.max()]

        job = Job(stream_runner.run, [jar, xml, df, num], queue=queue, engine=engine, mem_free='{}mb'.format(vmem))
        jobs.append(job)

    return jobs




@click.command()
@click.argument('earliest_night')
@click.argument('latest_night' )
@click.argument('data_dir', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE')
@click.option('--num_jobs', help='Number of jobs to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='400', type=click.INT)
@click.option("--log_level", type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--source',  help='Name of the source to analyze. e.g Crab', default='Crab')
@click.option('--max_delta_t', default=30,  help='Maximum time difference (minutes) allowed between drs and data files.', type=click.INT)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.password_option(help='password to read from the always awesome RunDB')
def main(earliest_night, latest_night, data_dir, jar, xml, out, queue, engine, num_jobs, vmem, log_level, port, source, max_delta_t, local, password):
    level=logging.INFO
    if log_level is 'DEBUG':
        level = logging.DEBUG
    elif log_level is 'WARN':
        level = logging.WARN
    elif log_level is 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +  '%(message)s'), level=level)

    #get data files
    jarpath = path.abspath(jar)
    xmlpath = path.abspath(xml)
    outpath = path.abspath(out)
    output_directory = path.dirname(outpath)
    #create dir if it doesnt exist
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Writing output and temporary data  to {}".format(output_directory))
    factdb = create_engine("mysql+pymysql://factread:{}@129.194.168.95/factdata".format(password))
    df = erna.load(earliest_night, latest_night, data_dir, source_name=source, timedelta_in_minutes=max_delta_t, factdb=factdb)

    job_list = make_jobs(jarpath, xmlpath, output_directory, df,  engine, queue, vmem, num_jobs)
    job_outputs = process_jobs(job_list, max_processes=num_jobs, local=local)
    logger.info("Concatenating results from each job and writing result to {}".format(out))
    frames = [f for f in job_outputs if isinstance(f, type(pd.DataFrame()))]
    if len(frames) != len(job_outputs):
        logger.warn("Only {} out of {} jobs returned a proper DataFrame.".format(len(frames), len(job_list)))

    if len(frames) == 0:
        return

    df = pd.concat(frames, ignore_index=True)
    logger.info("There are a total of {} events in the result".format(len(df)))

    name, extension = path.splitext(out)
    if extension == 'json':
        logger.info("Writing JSON to {}".format(out))
        df.to_json(out, orient='records', date_format='epoch' )
    elif extension == 'h5' or extension == 'hdf' or extension == 'hdf5':
        logger.info("Writing HDF5 to {}".format(out))
        df.to_hdf(out, 'table', mode='w')
    elif extension == 'csv':
        logger.info("Writing CSV to {}".format(out))
        df.to_csv(out)

if __name__ == "__main__":
    main()
