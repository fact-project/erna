import click
import logging
import time
import subprocess as sp

from .database import (
    database, setup_database, ProcessingState, Job
)
from .database_utils import update_job_status
from ..utils import load_config
from .job_monitor import JobMonitor
from .job_submitter import JobSubmitter
from .slurm import get_current_jobs

log = logging.getLogger(__name__)


def cancel_job(job):
    log.debug('Canceling job {}'.format(job.id))
    sp.run(['scancel', '--jobname=erna_{}'.format(job.id)])
    update_job_status(job, 'inserted')


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
    file_handler = logging.FileHandler(config['submitter'].pop('logfile'))
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s|%(levelname)s|%(message)s'
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
    job_submitter = JobSubmitter(**config['submitter'])

    log.info('Starting main loop')
    try:
        job_monitor.start()
        job_submitter.start()
        while True:
            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        log.info('Shutting done')
        job_submitter.terminate()
        job_submitter.join()

        queued = ProcessingState.select().where(ProcessingState.description == 'queued')
        running = ProcessingState.select().where(ProcessingState.description == 'running')

        with database.connection_context():
            log.info('Canceling queued jobs')
            for job in Job.select(Job.id).where(Job.status == queued):
                cancel_job(job)

        answer = click.confirm('Wait for running jobs? ')
        if answer:
            log.info('Waiting for running jobs to finish')
            try:
                n_running = (get_current_jobs().status == 'running').sum()
                while n_running > 0:
                    log.info('Waiting for {} jobs to finish'.format(n_running))
                    time.sleep(10)
            except (KeyboardInterrupt, SystemExit):
                log.info('Shutting done')

                for job in Job.select(Job.id).where(Job.status == running):
                    cancel_job(job)
        else:
            for job in Job.select(Job.id).where(Job.status == running):
                cancel_job(job)

        job_monitor.terminate()
        job_monitor.join()
