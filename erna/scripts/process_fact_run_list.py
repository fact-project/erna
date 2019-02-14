import logging
import click
import numpy as np
import pandas as pd
from os import path
import os
import erna
from erna.utils import create_filename_from_format
from erna import stream_runner as stream_runner_std
from erna import stream_runner_local_output as stream_runner_local
import gridmap
from gridmap import Job


logger = logging.getLogger(__name__)


def make_jobs(jar, xml, aux_source_path, output_directory, df_mapping,  engine, queue,
              vmem, num_jobs, walltime, output_path=None, filename_format="{basename}_{num}.json"):
    jobs = []

    if output_path:
        logger.info("Using stream runner for local output")
    else:
        logger.debug("Using std stream runner gathering output from all nodes")

    # create job objects
    split_indices = np.array_split(np.arange(len(df_mapping)), num_jobs)
    for num, indices in enumerate(split_indices):
        df = df_mapping[indices.min(): indices.max()]
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
            Job(
                stream_runner.run,
                run,
                queue=queue,
                walltime=walltime,
                engine=engine,
                mem_free='{}mb'.format(vmem)
            )
        )

    avg_num_files = np.mean([len(part) for part in split_indices])
    logger.info("Created {} jobs with {} files each.".format(len(jobs), avg_num_files))
    return jobs


@click.command()
@click.argument('file_list', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('aux_source', type=click.Path(exists=True, dir_okay=True, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE')
@click.option('--num_jobs', help='Number of jobs to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='400', type=click.INT)
@click.option("--log_level", type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
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
def main(file_list, jar, xml, aux_source, out, queue, walltime, engine, num_jobs, vmem, log_level, port, local, local_output, local_output_format):
    '''
    Specify the path to a .json file as created by the fetch_runs.py script via the FILE_LIST argument.
    num_jobs will be created and executed on the cluster.
    '''
    level = logging.INFO
    if log_level == 'DEBUG':
        level = logging.DEBUG
    elif log_level == 'WARN':
        level = logging.WARN
    elif log_level == 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +  '%(message)s'), level=level)

    name, extension = os.path.splitext(file_list)

    if extension not in ['.json', '.csv']:
        logger.error("Did not recognize file extension {}.".format(extension))
        exit(0)
    elif extension == '.json':
        logger.info("Reading JSON from {}".format(file_list))
        df = pd.read_json(file_list)
    elif extension == '.csv':
        logger.info("Reading CSV from {}".format(file_list))
        df = pd.read_csv(file_list)

    logger.info("Read {} runs".format(len(df)))

    # get data files
    jarpath = path.abspath(jar)
    xmlpath = path.abspath(xml)
    aux_source_path = path.abspath(aux_source)
    outpath = path.abspath(out)
    output_directory = path.dirname(outpath)
    # create dir if it doesnt exist
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Writing output and temporary data  to {}".format(output_directory))

    if local_output:
        job_list = make_jobs(
            jarpath,
            xmlpath,
            aux_source_path,
            output_directory,
            df,
            engine,
            queue,
            vmem,
            num_jobs,
            walltime,
            output_path=output_directory,
            filename_format=local_output_format
        )
    else:
        job_list = make_jobs(
            jarpath,
            xmlpath,
            aux_source_path,
            output_directory,
            df,
            engine,
            queue,
            vmem,
            num_jobs,
            walltime,
        )

    job_outputs = gridmap.process_jobs(job_list, max_processes=num_jobs, local=local)
    erna.collect_output(job_outputs, out, df)


if __name__ == "__main__":
    main()
