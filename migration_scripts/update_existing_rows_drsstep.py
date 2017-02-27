from erna.automatic_processing.database import database, RawDataFile, DrsFile
from tqdm import tqdm
import pandas as pd
from erna.utils import load_config, create_mysql_engine
import click


@click.command()
@click.option('-c', '--config')
def main(config):

    config = load_config(config)

    database.init(**config['processing_database'])
    database.connect()

    engine = create_mysql_engine(**config['fact_database'])

    df = pd.read_sql_table('RunInfo', engine, columns=[
        'fNight', 'fRunID', 'fDrsStep', 'fROI'
    ])

    df.set_index(['fNight', 'fRunID'], inplace=True)

    query = RawDataFile.select().where(RawDataFile.roi == None)
    for raw_data_file in tqdm(query, total=query.count()):

        night = raw_data_file.night.year * 10000 + raw_data_file.night.month * 100 + raw_data_file.night.day

        raw_data_file.roi = df.loc[(night, raw_data_file.run_id), 'fROI']
        raw_data_file.save()

    query = DrsFile.select().where(DrsFile.roi == None)
    for drs_file in tqdm(query, total=query.count()):

        night = drs_file.night.year * 10000 + drs_file.night.month * 100 + drs_file.night.day

        drs_file.roi = df.loc[(night, drs_file.run_id), 'fROI']
        drs_file.drs_step = df.loc[(night, drs_file.run_id), 'fDrsStep']
        drs_file.save()


if __name__ == '__main__':
    main()
