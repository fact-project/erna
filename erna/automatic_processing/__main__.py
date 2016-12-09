from erna.automatic_processing.database import (
    database, setup_database, Job, ProcessingState, RawDataFile
)
from .database_utils import count_jobs, get_pending_jobs
from .qsub import get_current_jobs, submit_job
from ..utils import load_config
from .job_monitor import JobMonitor
import click
import logging
import os
import time

log = logging.getLogger(__name__)


def process_pending_jobs(
        max_queued_jobs,
        data_directory,
        host,
        port,
        location='isdc',
        **kwargs
        ):
    current_jobs = get_current_jobs()
    running_jobs = current_jobs.query('state == "running"')
    queued_jobs = current_jobs.query('state == "pending"')
    log.debug('Currently {} jobs running'.format(len(running_jobs)))
    log.debug('Currently {} jobs queued'.format(len(queued_jobs)))
    log.debug('Currently {} pending jobs in database'.format(
        count_jobs(state='inserted')
    ))

    mail_address = kwargs.get('mail_address')
    mail_settings = kwargs.get('mail_settings', 'a')

    if len(queued_jobs) < max_queued_jobs:
        pending_jobs = get_pending_jobs(limit=max_queued_jobs - len(queued_jobs))

        for job in pending_jobs:
            try:
                submit_job(
                    job,
                    output_base_dir=os.path.join(data_directory, 'fact-tools'),
                    data_dir=data_directory,
                    mail_address=mail_address,
                    mail_settings=mail_settings,
                    submitter_host=host,
                    submitter_port=port,
                )
                log.info('New job with id {} queued'.format(job.id))
            except:
                log.exception('Could not submit job')
                job.status = ProcessingState.get(description='error')
                job.save()


@click.command()
@click.option(
    '--config', '-c',
    help='Config file, if not given, env ERNA_CONFIG and ./erna.yaml will be tried'
)
@click.option(
    '--verbose', '-v', help='Set log level of "erna" to debug', is_flag=True,
)
def main(config, verbose):
    config = load_config(config)

    logging.getLogger('erna').setLevel(logging.INFO)
    if verbose:
        logging.getLogger('erna').setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(config['submitter']['logfile'])
    formatter = logging.Formatter(
        '%(asctime)s|%(levelname)s|%(name)s|%(message)s'
    )

    for handler in (stream_handler, file_handler):
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

    log.info('Initialising database')
    database.init(**config['processing_database'])
    database.connect()
    setup_database(database)
    database.close()

    job_monitor = JobMonitor(port=config['submitter']['port'])

    log.info('Starting main loop')
    try:
        job_monitor.start()
        while True:
            database.connect()
            process_pending_jobs(**config['submitter'])
            database.close()
            time.sleep(config['submitter']['interval'])
    except (KeyboardInterrupt, SystemExit):
        job_monitor.terminate()
