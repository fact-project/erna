import logging
import click
import pandas as pd
import os

from ..logging import setup_logging
from ..jobs import make_jobs
from ..path import ensure_output, test_data_path
from ..dask import Cluster
from ..io import collect_output


logger = logging.getLogger(__name__)


@click.command()
@click.argument('file_list', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('aux_source', type=click.Path(exists=True, dir_okay=True, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option(
    '--walltime',
    default='02:00:00',
    help='Estimated maximum walltime of your job in format hh:mm:ss.',
)
@click.option(
    '--engine',
    type=click.Choice(['PBS', 'SGE', 'SLURM', 'LOCAL']),
    default='SLURM', help='Name of the grid engine used by the cluster.'
)
@click.option('--interface', help='Name of the network interface to use')
@click.option('--n-jobs', help='Number of jobs to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='400', type=click.INT)
@click.option("--log_level", type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option(
    '--local-output', default=False, is_flag=True,
    help=(
        'Flag indicating whether jobs write their output localy'
        ' to disk without gathering everything in the mother'
        ' process. In this case the output file only contains a'
        ' summary oth the processed jobs. The data ouput will be'
        ' in separate files'
    ),
    show_default=True
)
@click.option(
    '--local-output-format',
    default="{basename}_{num:03d}.json",
    help=(
        "Give the file format for the local output funktionality."
        " Default is: '{basename}_{num}.json'.Only works with option --local_output."
    )
)
@click.option('--yes', is_flag=True, help="No prompts for anything, always assume 'yes'")
def main(
    file_list,
    jar,
    xml,
    aux_source,
    out,
    queue,
    walltime,
    engine,
    interface,
    n_jobs,
    vmem,
    log_level,
    port,
    local_output,
    local_output_format,
    yes,
):
    '''
    Specify the path to a .json file as created by the `fetch_fact_runs`
    script via the FILE_LIST argument.
    num_jobs will be created and executed on the cluster.
    '''
    setup_logging(log_level)

    name, extension = os.path.splitext(file_list)

    if extension not in ['.json', '.csv']:
        logger.error("Did not recognize file extension {}.".format(extension))
        exit(1)
    elif extension == '.json':
        logger.info("Reading JSON from {}".format(file_list))
        df = pd.read_json(file_list)
    elif extension == '.csv':
        logger.info("Reading CSV from {}".format(file_list))
        df = pd.read_csv(file_list)

    logger.info("Read {} runs".format(len(df)))

    # get data files
    jar = os.path.abspath(jar)
    xml = os.path.abspath(xml)
    aux_path = os.path.abspath(aux_source)

    if local_output:
        name, _ = os.path.splitext(os.path.basename(out))
        outputbase = os.path.join(os.path.dirname(out), name)
        ensure_output(outputbase)
        out = None
    else:
        outputbase = None
        ensure_output(out)

    df = test_data_path(df, "data_path")

    mask = df['data_file_exists'].astype(bool)
    df_runs = df.loc[mask]
    df_runs_missing = df.loc[~mask]

    logger.warn("Missing {} dataruns due to missing datafiles".format(
        len(df_runs_missing)
    ))
    logger.info("Would process {} jobs with {:2f} runs per job".format(
        len(df_runs), len(df_runs) / n_jobs
    ))
    if not yes:
        click.confirm('Do you want to continue processing and start jobs?', abort=True)



    job_list, df_runs = make_jobs(
        jar=jar,
        xml=xml,
        data_paths=df['data_path'],
        drs_paths=df['drs_path'],
        vmem=vmem,
        aux_path=aux_path,
        n_jobs=n_jobs,
        walltime=walltime,
        outputbase=outputbase,
        local_output_format=local_output_format,
    )

    with Cluster(
            engine=engine,
            memory='{:.0f}M'.format(vmem),
            n_jobs=n_jobs,
            interface=interface,
            queue=queue,
    ) as cluster:
        futures = cluster.process_jobs(job_list)
        collect_output(futures, out)


if __name__ == "__main__":
    main()
