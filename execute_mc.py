import logging
import click
import numpy as np
import pandas as pd
from os import path
import erna
import gridmap
from gridmap import Job


logger = logging.getLogger(__name__)


def make_jobs(jar, xml, data_paths, drs_paths,  engine, queue, vmem, num_jobs):
    jobs = []
    # create job objects
    data_partitions = np.array_split(data_paths, num_jobs)
    drs_partitions = np.array_split(drs_paths, num_jobs)
    for num, (data, drs) in enumerate(zip(data_partitions, drs_partitions)):
        df = pd.DataFrame({'data_path':data, 'drs_path':drs})
        job = Job(erna.stream_runner.run, [jar, xml, df, num], queue=queue, engine=engine, mem_free='{}mb'.format(vmem))
        jobs.append(job)

    avg_num_files = np.mean([len(part) for part in data_partitions])
    logger.info("Created {} jobs with {} files each.".format(len(jobs), avg_num_files))
    return jobs



@click.command()
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True))
@click.argument('drs_file', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('mc_path',  nargs=-1,  type=click.Path(exists=True, file_okay=True, readable=True))
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE')
@click.option('--num_jobs', help='Number of jobs to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='400', type=click.INT)
@click.option("--log_level", type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy.')
def main( jar, xml, out,drs_file, mc_path, queue, engine, num_jobs, vmem, log_level, port, local):
    '''
    Script to execute fact-tools on MonteCarlo files. Use the MC_PATH argument to specifiy which files should be used.
    Pass a glob pattern to select more than one file.
    '''
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
    num_files = len(mc_path)
    logger.info("Found {} files.".format(num_files))
    if num_files == 1:
        logger.error("Need more than one file to work with.")
        return
    if num_jobs > num_files:
        logger.error("You specified more jobs than files. This doesn't make sense.")
        return


    mc_paths_array = np.array(mc_path)
    drs_paths_array = np.repeat(np.array(drs_file), len(mc_paths_array))

    job_list = make_jobs(jarpath, xmlpath, mc_paths_array, drs_paths_array,  engine, queue, vmem, num_jobs)

    job_outputs = gridmap.process_jobs(job_list, max_processes=num_jobs, local=local)
    erna.collect_output(job_outputs, out)

if __name__ == "__main__":
    main()