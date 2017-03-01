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

    run_type_key = IntegerField(null=True)
    run_type_name = CharField(null=True)

    migrate(
        migrator.add_column('raw_data_files', 'run_type_key', run_type_key),
        migrator.add_column('raw_data_files', 'run_type_name', run_type_name)
    )



if __name__ == '__main__':
    main()
