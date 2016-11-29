import xmltodict
import subprocess as sp
import os
import pandas as pd


def get_current_jobs(user=None):
    ''' Return a dataframe with current jobs of user '''
    user = user or os.environ['USER']
    xml = sp.check_output(['qstat', '-u', user, '-xml']).decode()
    data = xmltodict.parse(xml)
    job_list = data['job_info']['queue_info']['job_list']
    if not isinstance(job_list, list):
        job_list = [job_list]
    df = pd.DataFrame(job_list)

    df.drop('state', axis=1, inplace=True)
    df.rename(inplace=True, columns={
        '@state': 'state',
        'JB_owner': 'owner',
        'JB_name': 'name',
        'JB_job_number': 'job_number',
        'JAT_prio': 'priority',
        'JAT_start_time': 'start_time',
    })

    df = df.astype({'job_number': int, 'priority': float})
    df['start_time'] = pd.to_datetime(df['start_time'])
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
        jarfile,
        xmlfile,
        infile,
        drsfile,
        auxdir,
        outputpath,
        **kwargs
        ):

    executable = sp.check_output(
        ['which', 'erna_automatic_processing_executor']
    ).decode().strip()

    cmd = build_qsub_command(
        executable=executable,
        environment={
            'JARFILE': jarfile,
            'XMLFILE': xmlfile,
            'OUTPUTPATH': outputpath,
            'facttools_infile': 'file:' + infile,
            'facttools_drsfile': 'file:' + drsfile,
            'facttools_aux_dir': 'file:' + auxdir,
        },
        **kwargs,
    )

    sp.check_call(cmd)
