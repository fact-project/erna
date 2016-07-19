from glob import iglob
import os
import socket
import click
import re
from datetime import date
import yaml

from .database import basedirs, RawDataFile, DrsFile, database


@click.command
@click.option('--year', help='The year to update (default all)')
@click.option('--month', help='The month to update (default all)')
@click.option('--day', help='The day to update (default all)')
@click.option('--config', '-c', help='Yaml file containing database credentials')
def main(year, month, day, config):

    with open(config or 'config.yaml') as f:
        db_config = yaml.safe_load(f)

    database.init(**db_config)
    database.connect()

    if 'isdc' in socket.gethostname():
        basedir = basedirs['isdc']
        location = 'isdc'
    else:
        basedir = basedirs['phido']
        location = 'dortmund'

    pattern = os.path.join(
        basedir,
        year or '*',
        ('0' + month)[-2:] if month else '*',
        ('0' + day)[-2:] if day else '*',
        '*.fits.[fg]z'
    )

    for filename in iglob(pattern):

        f = RawDataFile.from_path(filename)
        if location == 'isdc':
            f.available_isdc = True
            f.save(only='available_isdc')
        if location == 'dortmund':
            f.available_dortmund = True
            f.save(only='available_dortmund')

    pattern = os.path.join(
        basedir,
        year or '*',
        ('0' + month)[-2:] if month else '*',
        ('0' + day)[-2:] if day else '*',
        '*.drs.fits.gz'
    )

    for filename in iglob(pattern):
        f = DrsFile.from_path(filename)
        if location == 'isdc':
            f.available_isdc = True
            f.save(only='available_isdc')
        if location == 'dortmund':
            f.available_dortmund = True
            f.save(only='available_dortmund')

    database.close()
