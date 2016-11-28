from IPython import embed
import click

from ..automatic_processing.database import database
from ..utils import load_config


@click.command()
@click.option('--config', '-c', help='Path to the yaml config file')
def main(config):
    config = load_config(config)

    database.init(**config['processing_database'])
    database.connect()

    embed()


if __name__ == '__main__':
    main()
