import logging
import click
import numpy as np
import pandas as pd
from os import path

import erna
from erna import stream_runner as stream_runner_std
from erna import stream_runner_local_output as stream_runner_local

import gridmap
from gridmap import Job
from tqdm import tqdm
import glob

logger = logging.getLogger(__name__)


def make_jobs(jar, xml, data_paths, drs_paths,
              engine, queue, vmem, num_jobs, walltime, output_path=None):
    jobs = []

    data_partitions = np.array_split(data_paths, num_jobs)
    drs_partitions = np.array_split(drs_paths, num_jobs)

    for num, (data, drs) in enumerate(zip(data_partitions, drs_partitions)):
        df = pd.DataFrame({'data_path': data, 'drs_path': drs})
        if output_path:
            file_name, _ = path.splitext(path.basename(output_path))
            file_name += "_{}.json".format(num)
            out_path = path.dirname(output_path)
            run = [jar, xml, df, path.join(out_path, file_name)]
            stream_runner = stream_runner_local
        else:
            run = [jar, xml, df]
            stream_runner = stream_runner_std

        jobs.append(
            Job(stream_runner.run,
                run,
                queue=queue,
                walltime=walltime,
                engine=engine,
                mem_free='{}mb'.format(vmem)
                )
            )

    avg_num_files = np.mean([len(part) for part in data_partitions])
    logger.info("Created {} jobs with {} files each.".format(len(jobs), avg_num_files))
    return jobs



@click.command()
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True))
@click.argument('mc_path',  nargs=-1,  type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short', show_default=True)
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00', show_default=True)
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE', show_default=True)
@click.option('--num_jobs', help='Number of jobs to start on the cluster.', default='4', type=click.INT, show_default=True)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='1000', type=click.INT, show_default=True)
@click.option("--log_level", type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy.',show_default=True)
@click.option('--local_output', default=False,is_flag=True,
              help='Flag indicating whether jobs write their output localy'
              + 'to disk without gathering everything in the mother'
              + 'process. In this case the output file only contains a'
              + 'summary oth the processed jobs. The data ouput will be'
              + 'inseparate files',
              show_default=True)
def main( jar, xml, out, mc_path, queue, walltime, engine, num_jobs, vmem, log_level, port, local, local_output):
    '''
    Script to execute fact-tools on MonteCarlo files. Use the MC_PATH argument to specifiy the folders containing the MC
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

    if local_output:
        name, _ = path.splitext(path.basename(out))
        local_output_dir = path.join(path.dirname(out), name)
        erna.ensure_output(local_output_dir)
    erna.ensure_output(out)

    jarpath = path.abspath(jar)
    xmlpath = path.abspath(xml)
    drspath = erna.mc_drs_file()
    logger.info('Using drs file at {}'.format(drspath))

    #get data files
    files=[]
    for folder in tqdm(mc_path):
        # print("Entering folder {}".format(folder))
        pattern = path.join(folder, '**/*_Events.fit*')
        f = glob.glob(pattern, recursive=True)
        files = files + f

    num_files = len(files)
    logger.info("Found {} files.".format(num_files))
    if num_files == 1:
        logger.error("Need more than one file to work with.")
        return
    if num_jobs > num_files:
        logger.error("You specified more jobs than files. This doesn't make sense.")
        return

    click.confirm('Do you want to continue processing and start jobs?', abort=True)

    mc_paths_array = np.array(files)
    drs_paths_array = np.repeat(np.array(drspath), len(mc_paths_array))

    if local_output:
        job_list = make_jobs(
                        jarpath, xmlpath, mc_paths_array,
                        drs_paths_array,  engine, queue,
                        vmem, num_jobs, walltime, output_path=local_output_dir
                        )
    else:
        job_list = make_jobs(
                        jarpath, xmlpath, mc_paths_array,
                        drs_paths_array,  engine, queue,
                        vmem, num_jobs, walltime
                        )

    job_outputs = gridmap.process_jobs(job_list, max_processes=num_jobs, local=local)
    erna.collect_output(job_outputs, out)

if __name__ == "__main__":
    main()
