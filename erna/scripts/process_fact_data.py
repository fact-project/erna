import logging
import click

import numpy as np
import sqlalchemy
import os

import gridmap
from gridmap import Job

import erna
from erna.utils import create_filename_from_format
from erna import stream_runner as stream_runner_std
from erna import stream_runner_local_output as stream_runner_local

import erna.datacheck_conditions as dcc

logger = logging.getLogger(__name__)


def make_jobs(jar, xml, aux_source_path, output_directory, df_mapping,  engine, queue, vmem, num_runs_per_bunch, walltime, output_path=None, filename_format="{basename}_{num}.json"):
    jobs = []

    if output_path:
        logger.info("Using stream runner for local output")
    else:
        logger.debug("Using std stream runner gathering output from all nodes")

    # create job objects

    df_mapping["bunch_index"]= np.arange(len(df_mapping)) // num_runs_per_bunch
    for num, df in df_mapping.groupby("bunch_index"):
        df=df.copy()
        df["bunch_index"] = num

        if output_path:
            # create the filenames for each single local run
            file_name, _ = path.splitext(path.basename(output_path))
            file_name = create_filename_from_format(filename_format, file_name, num)
            out_path = path.dirname(output_path)
            run = [jar, xml, df, path.join(out_path, file_name), aux_source_path]
            stream_runner = stream_runner_local
        else:
            run = [jar, xml, df, aux_source_path]
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
        
    avg_num_files = np.mean([len(part) for num, part in df_mapping.groupby("bunch_index")])
    logger.info("Created {} jobs with on average {} files each.".format(len(jobs), avg_num_files))

    return jobs

from fact_conditions import create_condition_set

@click.command()
@click.argument('earliest_night')
@click.argument('latest_night' )
@click.argument('data_dir', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('aux_source', type=click.Path(exists=True, dir_okay=True, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE')
@click.option('--num_runs', help='Number of num runs per bunch to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--source',  help='Name of the source to analyze. e.g Crab', default='Crab')
@click.option('--conditions', '-c', help='Name of the data conditions as given in datacheck_conditions.py e.g @standard or "fParameter < 42 "', default=['@standard'], multiple=True)
@click.option('--max_delta_t', default=30,  help='Maximum time difference (minutes) allowed between drs and data files.', type=click.INT)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.option('--local_output', default=False, is_flag=True,
              help='Flag indicating whether jobs write their output localy'
              + 'to disk without gathering everything in the mother'
              + 'process. In this case the output file only contains a'
              + 'summary oth the processed jobs. The data ouput will be'
              + 'in separate files',
              show_default=True)
@click.option('--local_output_format', default="{basename}_{num}.json", help="Give the file format for the local output funktionality."
              + "%b will replace the out filename and %[1-9]n the given local number."
              + "Default is: '{basename}_{num}.json'.Only works with option --local_output. ")
@click.option('--yes', help="Assume 'yes'if your asked to continue processing and start jobs", default=False, is_flag=True)
@click.password_option(help='password to read from the always awesome RunDB')
def main(earliest_night, latest_night, data_dir, jar, xml, aux_source, out, queue, walltime, engine, num_runs, vmem, log_level, port, source, conditions, max_delta_t, local, local_output, local_output_format, yes, password):

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
        name, _ = os.path.splitext(os.path.basename(out))
        local_output_dir = os.path.join(os.path.dirname(out), name)
        erna.ensure_output(local_output_dir)
    erna.ensure_output(out)

    jarpath = os.path.abspath(jar)
    xmlpath =os. path.abspath(xml)
    outpath = os.path.abspath(out)
    erna.ensure_output(out)
    aux_source_path = os.path.abspath(aux_source)
    output_directory = os.path.dirname(outpath)
    # create dir if it doesnt exist
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Writing output data  to {}".format(out))
    factdb = sqlalchemy.create_engine("mysql+pymysql://factread:{}@129.194.168.95/factdata".format(password))

    # create the set of conditions we want to use
    data_conditions = create_condition_set(conditions)

    df_runs = erna.load(earliest_night, latest_night, data_dir, source_name=source, timedelta_in_minutes=max_delta_t, factdb=factdb, data_conditions=data_conditions)

    # check for missing data and fix possible wrong file extension (.fz->.gz)
    df = erna.test_data_path(df_runs, "data_path")

    df_runs = df[df['data_file_exists']]
    df_runs_missing = df[np.logical_not(df['data_file_exists'])]

    logger.warn("Missing {} dataruns due to missing datafiles".format(len(df_runs_missing)))
    logger.info("Would process {} jobs with {} runs per job".format(len(df_runs)//num_runs, num_runs))
    if not yes:
        click.confirm('Do you want to continue processing and start jobs?', abort=True)

    if local_output:
        job_list = make_jobs(jarpath, xmlpath, aux_source_path,
                             output_directory, df_runs, engine, queue,
                             vmem, num_runs,  walltime,
                             output_path=local_output_dir,
                             filename_format=local_output_format
                             )
    else:
        job_list = make_jobs(jarpath, xmlpath, aux_source_path,
                             output_directory, df_runs, engine, queue,
                             vmem, num_runs,  walltime
                             )

    job_outputs = gridmap.process_jobs(job_list, max_processes=len(job_list), local=local)
    erna.collect_output(job_outputs, out, df_runs)

if __name__ == "__main__":
    main()
