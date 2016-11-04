import yaml
import logging
import socket

from erna.database import rawdirs

log = logging.getLogger()
log.setLevel(logging.INFO)

def load_config(config):
    with open(config or 'config.yaml') as f:
        log.debug('Reading config file {}'.format(f.name))
        config = yaml.safe_load(f)
    return config


def get_host_settings():
    settings = dict()
    if 'isdc' in socket.gethostname():
        log.info('Assuming ISDC')
        settings['basedir'] = rawdirs['isdc']
        settings['location'] = 'isdc'
        settings['fact_database']['host'] = 'lp-fact'
    else:
        log.info('Assuming PHIDO')
        settings['basedir'] = rawdirs['phido']
        settings['location'] = 'dortmund'

    return settings
