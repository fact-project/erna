import xmltodict
import subprocess as sp
import os
import pandas as pd


def parse_qstat_xml(user=None):
    user = user or os.environ['USER']
    xml = sp.check_output(['qstat', '-u', user, '-xml']).decode()
    data = xmltodict.parse(xml)
    df = pd.DataFrame.from_dict(data['job_info']['queue_info']['job_list'])

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
