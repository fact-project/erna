import subprocess as sp
import os
import logging
import pandas as pd

from .database import ProcessingState
from .database_utils import (
    build_output_base_name, build_output_directory_name,
    save_xml, save_jar
)
from io import StringIO


log = logging.getLogger(__name__)


def get_current_jobs(user=None):
    ''' Return a dataframe with current jobs of user '''
    user = user or os.environ['USER']
    fmt = '%i,%j,%P,%S,%T,%p,%u,%V'
    csv = StringIO(sp.check_output([
        'squeue', '-u', user, '-o', fmt
    ]).decode())

    df = pd.read_csv(csv)
    df.rename(inplace=True, columns={
        'STATE': 'state',
        'USER': 'owner',
        'NAME': 'name',
        'JOBID': 'job_number',
        'SUBMIT_TIME': 'submission_time',
        'PRIORITY': 'priority',
        'START_TIME': 'start_time',
        'PARTITION': 'queue',
    })
    df['state'] = df['state'].str.lower()
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['submission_time'] = pd.to_datetime(df['submission_time'])

    return df


def build_sbatch_command(
    executable,
    *args,
    stdout=None,
    stderr=None,
    job_name=None,
    queue=None,
    mail_address=None,
    mail_settings='FAIL',
    resources=None,
    walltime=None,
):
    command = []
    command.append('sbatch')

    if job_name:
        command.extend(['-J', job_name])

    if queue:
        command.extend(['-p', queue])

    if mail_address:
        command.append('--mail-user={}'.format(mail_address))

    command.append('--mail-type={}'.format(mail_settings))

    if stdout:
        command.extend(['-o', stdout])

    if stderr:
        command.extend(['-e', stderr])

    if resources:
        command.append('-l')
        command.append(','.join(
            '{}={}'.format(k, v)
            for k, v in resources.items()
        ))

    if walltime is not None:
        command.append('--time={}'.format(walltime))

    command.append(executable)
    command.extend(args)

    return command


def submit_job(
    job,
    script,
    raw_dir,
    aux_dir,
    erna_dir,
    submitter_host,
    submitter_port,
    group,
    **kwargs
):

    jar_file = save_jar(job.jar_id, erna_dir)
    xml_file = save_xml(job.xml_id, erna_dir)

    output_dir = build_output_directory_name(job, os.path.join(erna_dir, 'fact-tools'))
    output_basename = build_output_base_name(job)

    log_dir = build_output_directory_name(job, os.path.join(erna_dir, 'logs'))
    os.makedirs(log_dir, exist_ok=True)

    cmd = build_sbatch_command(
        script,
        job_name='erna_{}'.format(job.id),
        stdout=os.path.join(log_dir, output_basename + '.log'),
        walltime=job.walltime,
        **kwargs,
    )

    env = os.environ.copy()
    env.update({
        'JARFILE': jar_file,
        'XMLFILE': xml_file,
        'OUTPUTDIR': output_dir,
        'WALLTIME': str(job.walltime * 60),
        'SUBMITTER_HOST': submitter_host,
        'SUBMITTER_PORT': str(submitter_port),
        'facttools_infile': 'file:' + job.raw_data_file.get_path(basepath=raw_dir),
        'facttools_drsfile': 'file:' + job.drs_file.get_path(basepath=raw_dir),
        'facttools_aux_dir': 'file:' + aux_dir,
        'facttools_output_basename': output_basename,
        'ERNA_GROUP': str(group),
    })

    output = sp.check_output(
        cmd,
        env=env,
    )
    log.debug(output.decode().strip())

    job.status = ProcessingState.get(description='queued')
    job.save()
