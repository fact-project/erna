import subprocess as sp
import os
import logging

from .utils import get_aux_dir
from .database import ProcessingState
from .database_utils import (
    build_output_base_name, build_output_directory_name,
    save_xml, save_jar
)


log = logging.getLogger(__name__)


def build_sbatch_command(
    executable,
    *args,
    stdout=None,
    stderr=None,
    job_name=None,
    queue=None,
    mail_address=None,
    mail_settings='a',
    resources=None,
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

    command.append(executable)
    command.extend(args)

    return command


def build_automatic_processing_sbatch_command(
    queue,
    **kwargs
):

    executable = sp.check_output(
        ['which', 'erna_automatic_processing_executor']
    ).decode().strip()

    cmd = build_sbatch_command(
        executable=executable,
        queue=queue,
        **kwargs,
    )

    return cmd


def submit_job(
    job,
    output_base_dir,
    data_dir,
    submitter_host,
    submitter_port,
    group,
    **kwargs
):

    jar_file = save_jar(job.jar_id, data_dir)
    xml_file = save_xml(job.xml_id, data_dir)

    aux_dir = get_aux_dir(job.raw_data_file.night)
    output_dir = build_output_directory_name(job, output_base_dir)
    output_basename = build_output_base_name(job)

    log_dir = os.path.join(data_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    cmd = build_automatic_processing_sbatch_command(
        output_basename=output_basename,
        output_dir=output_dir,
        job_name='erna_{}'.format(job.id),
        stdout=os.path.join(log_dir, 'erna_{:08d}.log'.format(job.id)),
        queue=job.queue.name,
        walltime=job.queue.walltime,
        group=group,
        **kwargs,
    )

    env = os.environ.copy()
    env.update({
        'JARFILE': jar_file,
        'XMLFILE': xml_file,
        'OUTPUTDIR': output_dir,
        'WALLTIME': job.queue.walltime,
        'SUBMITTER_HOST': submitter_host,
        'SUBMITTER_PORT': submitter_port,
        'facttools_infile': 'file:' + job.raw_data_file.get_path(),
        'facttools_drsfile': 'file:' + job.drs_file.get_path(),
        'facttools_aux_dir': 'file:' + aux_dir,
        'facttools_output_basename': output_basename,
        'ERNA_GROUP': group,
    })

    output = sp.check_output(
        cmd,
        env=env,
    )
    log.debug(output.decode().strip())

    job.status = ProcessingState.get(description='queued')
    job.save()
