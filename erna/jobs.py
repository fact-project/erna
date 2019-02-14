from collections import namedtuple
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


Job = namedtuple(
    'Job',
    ['jar', 'xml', 'run_df', 'outputfile', 'mem', 'aux_path']
)


def make_jobs(
    jar,
    xml,
    data_paths,
    drs_paths,
    queue,
    vmem,
    n_jobs,
    walltime,
    aux_path=None,
    outputbase=None,
    local_output_format='{basename}_{num:03d}.json',
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
            outputfile = local_output_format.format(
                basename=outputbase,
                num=num,
            )
        else:
            outputfile = None

        jobs.append(Job(
            jar=jar,
            xml=xml,
            run_df=df,
            outputfile=outputfile,
            aux_path=aux_path,
            mem=vmem,
        ))

    avg_num_files = np.mean([len(part) for part in data_partitions])
    logger.info('Created {} jobs with {} files each.'.format(len(jobs), avg_num_files))

    return jobs, df_runs
