import logging
import click
import numpy as np
import pandas as pd
import os

from tqdm import tqdm
import glob
from collections import namedtuple

from ..path import ensure_output
from .. import mc_drs_file
from ..io import collect_output
from ..dask import Cluster


Job = namedtuple(
    'Job',
    ['jar', 'xml', 'run_df', 'outputfile', 'queue', 'walltime', 'mem', 'aux_path']
)


logger = logging.getLogger(__name__)


def make_jobs(
    jar,
    xml,
    data_paths,
    drs_paths,
    queue,
    vmem,
    n_jobs,
    walltime,
    outputbase=None,
    local_output_extension='.fits',
):

    jobs = []

    data_partitions = np.array_split(data_paths, n_jobs)
    drs_partitions = np.array_split(drs_paths, n_jobs)

    if outputbase:
        logger.info('Using stream runner for local output')
    else:
        logger.debug('Using std stream runner gathering output from all nodes')

    df_runs = pd.DataFrame()
    for num, (data, drs) in enumerate(zip(data_partitions, drs_partitions)):

        df = pd.DataFrame({'data_path': data, 'drs_path': drs})
        df['bunch_index'] = num

        df_runs = df_runs.append(df)

        if outputbase:
            outputfile = '{}_{}.{}'.format(outputbase, num, local_output_extension)
        else:
            outputfile = None

        jobs.append(Job(
            jar=jar,
            xml=xml,
            run_df=df,
            outputfile=outputfile,
            queue=queue,
            walltime=walltime,
            mem=vmem,
            aux_path=None,
        ))

    avg_num_files = np.mean([len(part) for part in data_partitions])
    logger.info('Created {} jobs with {} files each.'.format(len(jobs), avg_num_files))
    return jobs, df_runs


@click.command()
@click.argument('jar', type=click.Path(dir_okay=False, file_okay=True, readable=True))
@click.argument('xml', type=click.Path(dir_okay=False, file_okay=True, readable=True))
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@click.argument('mc_path',  nargs=-1,  type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE', 'SLURM', 'LOCAL']), default='SLURM', show_default=True)
@click.option('--interface', help='Name of the network interface to use')
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short', show_default=True)
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00', show_default=True)
@click.option('--n-jobs', help='Number of jobs to start on the cluster.', default=4, type=click.INT, show_default=True)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default=1000, type=click.INT, show_default=True)
@click.option('--log-level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='Set output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option(
    '--local-output', is_flag=True,
    help=(
        'Flag indicating whether jobs write their output localy'
        ' to disk without gathering everything in the mother'
        ' process. In this case the output file only contains a'
        ' summary oth the processed jobs. The data ouput will be'
        ' in separate files'
    ),
    show_default=True,
)
@click.option('--mcdrs', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.option('--mcwildcard', help='Gives the wildcard for searching the folder for files.', type=click.STRING, default='**/*_Events.fit*')
@click.option('--local_output_extension', default='json', help='Give the file format for the local output funktionality.')
@click.option('--yes', help='Do not ask for permission', default=False, is_flag=True)
@click.option('--max-files', help='Maximum number of files to process', type=int)
def main(jar, xml, out, mc_path, engine, interface, queue, walltime, n_jobs, vmem, log_level, port, local_output, mcdrs, mcwildcard, local_output_extension, yes, max_files):
    '''
    Script to execute fact-tools on MonteCarlo files. Use the MC_PATH argument to specifiy the folders containing the MC
    '''
    level = logging.INFO
    if log_level == 'DEBUG':
        level = logging.DEBUG
    elif log_level == 'WARN':
        level = logging.WARN
    elif log_level == 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - ' + '%(message)s',
        level=level,
    )

    if local_output:
        name, _ = os.path.splitext(os.path.basename(out))
        outputbase = os.path.join(os.path.dirname(out), name)
        ensure_output(outputbase)
        out = None
    else:
        outputbase = None
        ensure_output(out)

    jarpath = os.path.abspath(jar)
    xmlpath = os.path.abspath(xml)
    drspath = mcdrs or mc_drs_file
    logger.info('Using drs file at {}'.format(drspath))

    # get data files
    files = []
    for folder in tqdm(mc_path):
        pattern = os.path.join(folder, mcwildcard)
        files.extend(glob.glob(pattern, recursive=True))

    if max_files is not None:
        files = files[:max_files]

    num_files = len(files)
    logger.info('Found {} files.'.format(num_files))
    if num_files == 1:
        logger.error('Need more than one file to work with.')
        return

    if n_jobs > num_files:
        logger.error('You specified more jobs than files. This doesn\'t make sense.')
        return

    if not yes:
        click.confirm('Do you want to continue processing and start jobs?', abort=True)

    mc_paths_array = np.array(files)
    drs_paths_array = np.repeat(np.array(drspath), len(mc_paths_array))

    job_list, df_runs = make_jobs(
        jar=jarpath,
        xml=xmlpath,
        data_paths=mc_paths_array,
        drs_paths=drs_paths_array,
        queue=queue,
        vmem=vmem,
        n_jobs=n_jobs,
        walltime=walltime,
        outputbase=outputbase,
        local_output_extension=local_output_extension,
    )

    with Cluster(
            engine=engine,
            memory='{:.0f}M'.format(vmem),
            n_jobs=n_jobs,
            interface=interface,
    ) as cluster:
        futures = cluster.process_jobs(job_list)
        collect_output(futures, out)


if __name__ == '__main__':
    main()
