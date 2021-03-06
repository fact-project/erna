import yaml
import os
import logging
from sqlalchemy import create_engine
import grp
import pwd
import subprocess
from datetime import date
import re

log = logging.getLogger(__name__)


def load_config(filename=None):
    '''
    load a yaml config file

    If filename is not given, the function looks first if there
    is an ERNA_CONFIG environment variable then if there is an `erna.yaml` in
    the current directory
    '''
    if filename is None:
        if 'ERNA_CONFIG' in os.environ:
            filename = os.environ['ERNA_CONFIG']
        elif os.path.isfile('erna.yaml'):
            filename = 'erna.yaml'
        else:
            raise ValueError('No config file found')

    log.debug('Loading config file {}'.format(filename))

    with open(filename, 'r') as f:
        config = yaml.safe_load(f)

    return config


def create_mysql_engine(user, password, host, database, port=3306):
    return create_engine(
        'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'.format(
            user=user,
            password=password,
            host=host,
            database=database,
            port=port,
        )
    )


def chown(path, username=None, groupname=None):
    '''
    Change ownership of given path to username:groupname
    '''
    uid = pwd.getpwnam(username).pw_uid if username else -1
    gid = grp.getgrnam(groupname).gr_gid if groupname else -1
    os.chown(path, uid, gid)


def night_int_to_date(night):
    ''' Convert the crazy FACT int to da date instance'''
    return date(night // 10000, (night % 10000) // 100, night % 100)


def date_to_night_int(night):
    ''' convert a date or datetime instance to the crazy FACT int '''
    return 10000 * night.year + 100 * night.month + night.day


def assemble_facttools_call(jar, xml, input_path, output_path, aux_source_path=None):
    ''' Assemble the call for fact-tools with the given combinations
    of jar, xml, input_path and output_path. The db_path is optional
    for the case where a db_file is needed
    '''
    call = [
            'java',
            '-XX:MaxHeapSize=1024m',
            '-XX:InitialHeapSize=512m',
            '-XX:CompressedClassSpaceSize=64m',
            '-XX:MaxMetaspaceSize=128m',
            '-XX:+UseConcMarkSweepGC',
            '-XX:+UseParNewGC',
            '-jar',
            jar,
            xml,
            '-Dinput=file:{}'.format(input_path),
            '-Doutput=file:{}'.format(output_path),
    ]

    if aux_source_path is not None:
        call.append('-Daux_source=file:{}'.format(aux_source_path))
    return call


def check_environment_on_node():
    ''' Check memory, java executalbe and version'''
    subprocess.check_call(['which', 'java'])
    subprocess.check_call(['free', '-m'])
    subprocess.check_call(['java', '-Xmx512m', '-version'])


def create_filename_from_format(filename_format, basename, num):
    """
    Given a special format string, create a filename_format with the basename and a given number.
    There are two named variables that can be used, one is basename which inserts the basename
    and the second one is num which is mandatory.
    """
    m = re.search('\{num', filename_format)
    if not m:
        raise ValueError("Missing named placeholder 'num' in format string")
    return filename_format.format({"basename":basename, "num":num})
