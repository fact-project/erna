import logging
import click

import numpy as np
import sqlalchemy
import os

import erna
import time

import erna.datacheck_conditions as dcc
from distributed import Executor

logger = logging.getLogger(__name__)

def test_function(properties):
    time.sleep(5)
    print("test")
    return properties['job_number']


def make_jobs(jar, xml, df_mapping, num_jobs):
    jobs = []
    # create job objects
    split_indices = np.array_split(np.arange(len(df_mapping)), num_jobs)
    for num, indices in enumerate(split_indices):
        runs = df_mapping[indices.min(): indices.max()]
        properties = {
            'jar':jar,
            'xml':xml,
            'job_number':num,
            'fact_runs':runs,
        }
        jobs.append(properties)
    return jobs


@click.command()
@click.argument('earliest_night')
@click.argument('latest_night' )
@click.argument('data_dir', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--num_jobs', help='Number of jobs to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--source',  help='Name of the source to analyze. e.g Crab', default='Crab')
@click.option('--conditions',  help='Name of the data conditions as given in datacheck_conditions.py e.g std', default='std')
@click.option('--max_delta_t', default=30,  help='Maximum time difference (minutes) allowed between drs and data files.', type=click.INT)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.password_option(help='password to read from the always awesome RunDB')
def main(earliest_night, latest_night, data_dir, jar, xml, out, queue, walltime, num_jobs, vmem, log_level, port, source, conditions, max_delta_t, local, password):
    executor = Executor('127.0.0.1:5678')
    level=logging.INFO
    if log_level is 'DEBUG':
        level = logging.DEBUG
    elif log_level is 'WARN':
        level = logging.WARN
    elif log_level is 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +  '%(message)s'), level=level)

    jarpath = os.path.abspath(jar)
    xmlpath = os.path.abspath(xml)
    outpath = os.path.abspath(out)
    erna.ensure_output(out)

    output_directory = os.path.dirname(outpath)
    #create dir if it doesnt exist
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Writing output data  to {}".format(out))
    factdb = sqlalchemy.create_engine("mysql+pymysql://factread:{}@129.194.168.95/factdata".format(password))
    data_conditions=dcc.conditions[conditions]
    df_runs = erna.load(earliest_night, latest_night, data_dir, source_name=source, timedelta_in_minutes=max_delta_t, factdb=factdb, data_conditions=data_conditions)

    click.confirm('Do you want to continue processing and start jobs?', abort=True)


    job_list = make_jobs(jarpath, xmlpath, df_runs, num_jobs)

    nums  = executor.map(test_function, job_list)
    l = executor.gather(nums)
    print(l)

if __name__ == "__main__":
    main()
