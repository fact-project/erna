from IPython import embed
import click
from datetime import date

from ..automatic_processing.database import *
from ..automatic_processing.database_utils import *
from ..utils import load_config


@click.command()
@click.option('--config', '-c', help='Path to the yaml config file')
def main(config):
    ''' Starts an IPython shell with helpful imports to work with erna '''
    config = load_config(config)

    database.init(**config['processing_database'])
    database.connect()

    embed()


if __name__ == '__main__':
    main()
