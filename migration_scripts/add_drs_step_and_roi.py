from erna.automatic_processing.database import database
from erna.utils import load_config
import click

from playhouse.migrate import MySQLMigrator, migrate
from peewee import IntegerField, CharField


@click.command()
@click.option('--config', '-c', help='Path to the yaml config file')
def main(config):
    config = load_config(config)
    database.init(**config['processing_database'])
    database.connect()

    migrator = MySQLMigrator(database)

    drs_step = IntegerField(null=True)
    roi = IntegerField(null=True)

    migrate(
        migrator.add_column('raw_data_files', 'roi', roi),
        migrator.add_column('drs_files', 'roi', roi),
        migrator.add_column('drs_files', 'drs_step', drs_step),
    )


if __name__ == '__main__':
    main()
