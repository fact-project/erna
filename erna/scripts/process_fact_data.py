import logging
import click

import os

from fact_conditions import create_condition_set

from ..factdb import get_run_data, create_fact_engine
from ..path import test_data_path
from ..jobs import make_jobs
from ..path import ensure_output
from ..logging import setup_logging
from ..dask import Cluster
from ..io import collect_output

logger = logging.getLogger(__name__)


@click.command()
@click.argument('earliest_night')
@click.argument('latest_night' )
@click.argument('data_dir', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('aux_path', type=click.Path(exists=True, dir_okay=True, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE', 'SLURM', 'LOCAL']), default='SLURM')
@click.option('--interface', help='Name of the network interface to use')
@click.option('--n-jobs', help='Number of jobs to start on the cluster.', default=4, type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--source',  help='Name of the source to analyze. e.g Crab', default='Crab')
@click.option('--conditions', '-c', help='Name of the data conditions as given in datacheck_conditions.py e.g @standard or "fParameter < 42 "', default=['@standard'], multiple=True)
@click.option('--max-delta-t', default=30,  help='Maximum time difference (minutes) allowed between drs and data files.', type=click.INT)
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
        " %b will replace the out filename and %[1-9]n the given local number."
        " Default is: '{basename}_{num}.json'.Only works with option --local_output."
    )
)
@click.option('--yes', is_flag=True, help="No prompts for anything, always assume 'yes'")
@click.password_option(help='password to read from the always awesome RunDB')
def main(
    earliest_night,
    latest_night,
    data_dir,
    jar,
    xml,
    aux_path,
    out,
    queue,
    walltime,
    engine,
    interface,
    n_jobs,
    vmem,
    log_level,
    port,
    source,
    conditions,
    max_delta_t,
    local,
    local_output,
    local_output_format,
    yes,
    password,
):

    setup_logging(log_level)

    if local_output:
        name, _ = os.path.splitext(os.path.basename(out))
        outputbase = os.path.join(os.path.dirname(out), name)
        ensure_output(outputbase)
        out = None
    else:
        outputbase = None
        ensure_output(out)

    jar = os.path.abspath(jar)
    xml = os.path.abspath(xml)
    aux_path = os.path.abspath(aux_path)
    factdb = create_fact_engine(password)

    # create the set of conditions we want to use
    data_conditions = create_condition_set(conditions)

    df_runs = get_run_data(
        earliest_night,
        latest_night,
        data_dir,
        ource_name=source,
        timedelta_in_minutes=max_delta_t,
        factdb=factdb,
        data_conditions=data_conditions,
    )

    # check for missing data and fix possible wrong file extension (.fz->.gz)
    df = test_data_path(df_runs, "data_path")

    df_runs = df[df['data_file_exists']]
    df_runs_missing = df[~df['data_file_exists']]

    logger.warn("Missing {} dataruns due to missing datafiles".format(
        len(df_runs_missing)
    ))
    logger.info("Would process {} jobs with {:2f} runs per job".format(
        len(df_runs), len(df_runs) / n_jobs
    ))

    if not yes:
        click.confirm('Do you want to continue processing and start jobs?', abort=True)

    job_list = make_jobs(
        jar=jar,
        xml=xml,
        data_paths=df_runs['data_path'],
        drs_paths=df_runs['drs_path'],
        vmem=vmem,
        aux_path=aux_path,
        n_jobs=n_jobs,
        walltime=walltime,
        outputbase=outputbase,
        filename_format=local_output_format,
        local_output_format=local_output_format,
    )

    with Cluster(
            engine=engine,
            memory='{:.0f}M'.format(vmem),
            n_jobs=n_jobs,
            interface=interface,
    ) as cluster:
        futures = cluster.process_jobs(job_list)
        collect_output(futures, out)


if __name__ == "__main__":
    main()
