import logging
import click
import erna
import numpy as np
import subprocess
import pandas as pd
from IPython import embed
from datetime import datetime
from os import path
import os

from gridmap import Job, process_jobs


logger = logging.getLogger(__name__)
def make_jobs(jar, xml, df_mapping,  engine, queue, vmem, num_jobs):
    jobs = []
    # create job objects
    split_indices = np.array_split(np.arange(len(df_mapping)), num_jobs)
    for num, indices in enumerate(split_indices):
        df = df_mapping[indices.min(): indices.max()]

        job = Job(job_function, [jar, xml, df, num], queue=queue, engine=engine, mem_free='{}mb'.format(vmem))
        jobs.append(job)

    return jobs


def job_function(jar, xml, df, num):
    name = xml.split(sep='.')[0]
    input_filename = "input_{}_{}.json".format(name ,num)
    output_filename = "output_{}_{}.json".format(name, num)
    # logger.info("Writing {} entries to json file  {}".format(len(df), filename))
    df.to_json(input_filename, orient='records', date_format='epoch' )
    call = [
            'java',
            '-XX:MaxHeapSize=1024m',
            '-XX:InitialHeapSize=512m',
            '-XX:CompressedClassSpaceSize=64m',
            '-XX:MaxMetaspaceSize=128m',
            '-XX:+UseConcMarkSweepGC',
            '-XX:+UseParNewGC',
            '-jar',
            jar,
            xml,
            '-Dinput=file:{}'.format(input_filename),
            '-Doutput=file:{}'.format(output_filename),
    ]

    subprocess.check_call(['which', 'java'])
    subprocess.check_call(['free', '-m'])
    subprocess.check_call(['java', '-version'])

    if 'SGE_ROOT' in os.environ:
        print(os.environ['SGE_ROOT'])
    else:
        print("could not resolve SGE_ROOT")

    print("Calling fact-tools:")
    print(call)
    subprocess.check_call(call)
    df_out = pd.read_json(output_filename)
    return df_out

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
def main(earliest_night, latest_night, data_dir, jar, xml, out, queue, engine, num_jobs, vmem, log_level, port, source, max_delta_t, local):
    out_path = out
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
    df = erna.load(earliest_night, latest_night, data_dir, source_name=source, timedelta_in_minutes=max_delta_t)
    jarpath = path.abspath(jar)
    xmlpath = path.abspath(xml)
    job_list = make_jobs(jarpath, xmlpath, df,  engine, queue, vmem, num_jobs)
    # if local :
    #     print('Running jobs locally')
    # else:
    #     print("Sending function jobs to cluster engine: {}. Into queue: {} \n".format(engine, queue))


    job_outputs = process_jobs(job_list, max_processes=4, local=local)
    df = pd.concat(job_outputs, ignore_index=True)
    logger.info("Concatenating results from each job and writing result to {}".format(out))
    df.to_json(out, orient='records', date_format='epoch' )


if __name__ == "__main__":
    main()
