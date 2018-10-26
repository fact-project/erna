import click
import pandas as pd

from ..automatic_processing.database_utils import insert_new_jobs
from ..utils import load_config
from datetime import date

from ..automatic_processing.database import (
    database,
    RawDataFile,
    Jar,
    XML,
    Job,
)


@click.command()
@click.argument('runlist')
@click.argument('jar')
@click.argument('xml')
@click.option(
    '-p', '--priority', default=5, type=int,
    help='Priority of the jobs, lower value means more important'
)
@click.option(
    '-w', '--walltime', default=60,
    help='Walltime for the jobs'
)
@click.option('--config', '-c', help='Path to the yaml config file')
def main(runlist, jar, xml, priority, queue, config):
    '''
    Submit automatic processing jobs for a given runlist

    Arguments

    RUNLIST: csv file with columns `night, run_id`
    JAR: version of the fact-tools jar
    XML: Name of the xml file to use

    Jar and XML must be uploaded to the processing db using erna_upload
    '''
    config = load_config(config)

    database.init(**config['processing_database'])

    jar = Jar.select(Jar.id, Jar.version).where(Jar.version == jar).get()
    xml = XML.get(name=xml, jar=jar)

    runs = pd.read_csv(runlist)
    runs['year'] = runs['night'] // 10000
    runs['month'] = ((runs['night'] % 10000) // 100)
    runs['day'] = (runs['night'] % 100)

    files = [
        RawDataFile.get(night=date(row.year, row.month, row.day), run_id=row.run_id)
        for row in runs.itertuples()
    ]

    insert_new_jobs(files, xml=xml, jar=jar, walltime=walltime)


if __name__ == '__main__':
    main()
