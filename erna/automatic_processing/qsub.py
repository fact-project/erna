import xmltodict
import subprocess as sp
import os
import pandas as pd

from .utils import save_xml, save_jar, get_aux_dir
from .utils import build_output_base_name, build_output_directory_name
from .database import ProcessingState


def get_current_jobs(user=None):
    ''' Return a dataframe with current jobs of user '''
    user = user or os.environ['USER']
    xml = sp.check_output(['qstat', '-u', user, '-xml']).decode()
    data = xmltodict.parse(xml)
    job_info = data['job_info']
    queue_info = job_info['queue_info']
    job_info = job_info['job_info']
    queued_jobs = queue_info['job_list'] if queue_info else []
    running_jobs = job_info['job_list'] if job_info else []

    df = pd.DataFrame(columns=[
        '@state', 'JB_job_number', 'JAT_prio', 'JB_name', 'JB_owner',
        'state', 'JB_submission_time', 'queue_name', 'slots', 'JAT_start_time'
    ])

    if not isinstance(running_jobs, list):
        running_jobs = [running_jobs]
    if not isinstance(queued_jobs, list):
        queued_jobs = [queued_jobs]

    df = df.append(pd.DataFrame(running_jobs + queued_jobs), ignore_index=True)

    if len(df) == 0:
        return df

    df.drop('state', axis=1, inplace=True)
    df.rename(inplace=True, columns={
        '@state': 'state',
        'JB_owner': 'owner',
        'JB_name': 'name',
        'JB_job_number': 'job_number',
        'JB_submission_time': 'submission_time',
        'JAT_prio': 'priority',
        'JAT_start_time': 'start_time',
    })

    df = df.astype({'job_number': int, 'priority': float})
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['submission_time'] = pd.to_datetime(df['submission_time'])
    return df


def build_qsub_command(
        executable,
        stdout=None,
        stderr=None,
        job_name=None,
        queue='fact_short',
        mail_address=None,
        mail_settings='a',
        environment=None,
        resources=None,
        engine='SGE',
        ):
    command = []
    command.append('qsub')

    if job_name:
        command.extend(['-N', job_name])

    command.extend(['-q', queue])
    if mail_address:
        command.extend(['-M', mail_address])

    command.extend(['-m', mail_settings])

    # allow a binary executable
    if engine == 'SGE':
        command.extend(['-b', 'yes'])

    if stdout:
        command.extend(['-o', stdout])

    if stderr:
        command.extend(['-e', stderr])

    if environment:
        command.append('-v')
        command.append(','.join(
            '{}={}'.format(k, v)
            for k, v in environment.items()
        ))

    if resources:
        command.append('-l')
        command.append(','.join(
            '{}={}'.format(k, v)
            for k, v in resources.items()
        ))

    command.append(executable)

    return command


def submit_fact_tools(
        jar_file,
        xml_file,
        in_file,
        drs_file,
        aux_dir,
        output_dir,
        output_basename,
        **kwargs
        ):

    executable = sp.check_output(
        ['which', 'erna_automatic_processing_executor']
    ).decode().strip()

    cmd = build_qsub_command(
        executable=executable,
        environment={
            'JARFILE': jar_file,
            'XMLFILE': xml_file,
            'OUTPUTDIR': output_dir,
            'facttools_infile': 'file:' + in_file,
            'facttools_drsfile': 'file:' + drs_file,
            'facttools_aux_dir': 'file:' + aux_dir,
            'facttools_output_basename': output_basename,
        },
        **kwargs
    )

    sp.check_call(cmd)


def submit_fact_tools_db_run(fact_tools_run, output_base_dir, data_dir, location='isdc'):

    jar_file = save_jar(fact_tools_run.fact_tools_version, data_dir)
    xml_file = save_xml(fact_tools_run.xml, data_dir)

    aux_dir = get_aux_dir(fact_tools_run.raw_data_file.night, location=location)
    output_dir = build_output_directory_name(fact_tools_run, output_base_dir)
    output_basename = build_output_base_name(fact_tools_run)

    submit_fact_tools(
        jar_file=jar_file,
        xml_file=xml_file,
        in_file=fact_tools_run.raw_data_file.get_path(location=location),
        drs_file=fact_tools_run.drs_file.get_path(location=location),
        aux_dir=aux_dir,
        output_basename=output_basename,
        output_dir=output_dir,
        name='erna_{}'.format(fact_tools_run.id),
    )
    fact_tools_run.status = ProcessingState.get(description='queued')
    gact_tools_run.save()
