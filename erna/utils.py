import yaml
import os
import logging
from sqlalchemy import create_engine

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
