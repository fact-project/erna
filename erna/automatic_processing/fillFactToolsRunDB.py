import pandas as pd
from dateutil import parser as date_parser
import click
import yaml
import logging

from sqlalchemy import create_engine
import os


from dateutil.parser import parse as parse_date
from peewee import SQL
from erna.database import database, RawDataFile, DrsFile, FactToolsRun, night_int_to_date, facttoolsdirs
from erna.automatic_processing import load_config, get_host_settings

from IPython import embed

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s|%(levelname)s|%(name)s|%(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

facttools_link = "https://github.com/fact-project/fact-tools/releases/download/v0.16.2/fact-tools-{}.jar"

def select_drs_file(drs_query, run):
    drs_files = drs_query.where(DrsFile.night == run.night)
    drs_files_before = drs_files.where(DrsFile.run_id < run.run_id)
    drs_files_after = drs_files.where(DrsFile.run_id > run.run_id)

    drs_files_before = drs_files_before.order_by(DrsFile.run_id.desc())
    drs_files_after = drs_files_after.order_by(DrsFile.run_id)

    if drs_files_before.count() > 0:
        drs_files = drs_files_before
    elif drs_files_after > 0:
        drs_files = drs_files_after

    if drs_files.count() > 0:
        log.debug("#DrsFiles:{}".format(drs_files.count()))
        drs = drs_files.first()
        log.debug("raw file:{}_{}".format(run.night, run.run_id))
        log.debug("Drs file:{}_{}".format(drs.night, drs.run_id))
        return drs

    return None


@click.command()
@click.option('--start', type=click.INT, default=None)
@click.option('--end', type=click.INT, default=None)
@click.option('--out', type=click.Path(exists=False, dir_okay=False), default="/fact/fact-tools")
@click.option('--priority',
              default=1,
              help='Priority of the runs',
              type=click.INT)
@click.option('--ftversion',
              default="v0.16.2",
              help='fact-tools version',
              type=click.STRING)
@click.option('--location',
            default="isdc",
            help='location to run for',
            type=click.STRING)
@click.option('--verbose', '-v', help='Set logging level to DEBUG', is_flag=True)
@click.option('--config', '-c', help='Yaml file containing database credentials')


def main(start, end, out, priority, ftversion, location, verbose, config):
    if verbose:
        log.setLevel(logging.DEBUG)
        logging.captureWarnings(True)

    config = load_config(config=config)
    database.init(**config['processing_database'])
    database.connect()

    log.info('Start: {}'.format(start))
    log.info('End: {}'.format(end))
    log.info('Outputpath: {}'.format(out))
    log.info('Priority: {}'.format(priority))
    log.info('ftversion: {}'.format(ftversion))


    # if location == "isdc":
    #     config['fact_database']['host'] = 'lp-fact'

    #TODO: Check that location is valid

    raw_query = RawDataFile.select()
    drs_query = DrsFile.select()
    if location == "isdc":
        log.info('Location: {}'.format(location))
        raw_query = raw_query.where(RawDataFile.available_isdc)
        drs_query = drs_query.where(DrsFile.available_isdc)
    elif location == "dortmund":
        raw_query = raw_query.where(RawDataFile.available_dortmund)
        drs_query = drs_query.where(DrsFile.available_dortmund)

    basedir = facttoolsdirs[location]

    if start:
        log.info('Begin: {}'.format(start))
        raw_query = raw_query.where(RawDataFile.night >= night_int_to_date(start))
        drs_query = drs_query.where(DrsFile.night >= night_int_to_date(start))

    if end:
        raw_query = raw_query.where(RawDataFile.night <= night_int_to_date(end))
        drs_query = drs_query.where(DrsFile.night <= night_int_to_date(end))

    # query FactToolsRun for all runs with the given version
    # ft_query = (FactToolsRun
    #             .select(FactToolsRun.raw_data_id)
    #             .where(FactToolsRun.fact_tools_version == ftversion)
    #             )
    # TODO: check for excisting runs in ft_query if ft_query has content
    # if ft_query.count() != 0:
    # TODO: Only add the difference of raw_query and ft_query
    # missing_runs = (raw_query - ft_query).order_by(SQL('night'))

    for run in raw_query.limit(5):
        raw_path = run.get_path(location=location)
        drs_file = select_drs_file(drs_query, run)



    embed()


if __name__ == '__main__':
    main()
