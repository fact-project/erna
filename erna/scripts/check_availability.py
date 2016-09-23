from glob import iglob
import os
import socket
import click
import yaml
import logging

from erna.database import rawdirs, RawDataFile, DrsFile, database, drsfile_re, datafile_re

log = logging.getLogger('erna')
log.setLevel(logging.INFO)


@click.command()
@click.option('--year', help='The year to update (default all)')
@click.option('--month', help='The month to update (default all)')
@click.option('--day', help='The day to update (default all)')
@click.option('--config', '-c', help='Yaml file containing database credentials')
@click.option('--verbose', '-v', help='Set logging level to DEBUG', is_flag=True)
def main(year, month, day, config, verbose):

    if verbose:
        log.setLevel(logging.DEBUG)

        logging.captureWarnings(True)
        logging.basicConfig(format=('%(asctime)s - %(levelname)s - ' + '%(message)s'))

    with open(config or 'config.yaml') as f:
        log.debug('Reading config file {}'.format(f.name))
        db_config = yaml.safe_load(f)

    log.debug('Connecting to database')
    database.init(**db_config)
    database.connect()
    log.info('Database connection established')

    if 'isdc' in socket.gethostname():
        log.info('Assuming ISDC')
        basedir = rawdirs['isdc']
        location = 'isdc'
    else:
        log.info('Assuming PHIDO')
        basedir = rawdirs['phido']
        location = 'dortmund'

    pattern = os.path.join(
        basedir,
        year or '*',
        ('0' + month)[-2:] if month else '*',
        ('0' + day)[-2:] if day else '*',
        '*.fits.[fg]z'
    )

    for filename in iglob(pattern):

        if datafile_re.match(filename):
            f = RawDataFile.from_path(filename)

        elif drsfile_re.match(filename):
            f = DrsFile.from_path(filename)
        else:
            continue

        if location == 'isdc':
            f.available_isdc = True
            f.save(only=[
                RawDataFile.night, RawDataFile.run_id, RawDataFile.available_isdc
            ])

        if location == 'dortmund':
            f.available_dortmund = True
            f.save(only=[
                RawDataFile.night, RawDataFile.run_id, RawDataFile.available_dortmund
            ])

        log.debug('Updated availability of file {}'.format(f.basename))

    database.close()


if __name__ == '__main__':
    main()
