from erna.automatic_processing.database import (
    database, setup_database, ProcessingState, Job
)
from ..utils import load_config
from .job_monitor import JobMonitor
from .job_submitter import JobSubmitter
import click
import logging
import time
import subprocess as sp

log = logging.getLogger(__name__)


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
    job_submitter = JobSubmitter(**config['submitter'])

    log.info('Starting main loop')
    try:
        job_monitor.start()
        job_submitter.start()
        while True:
            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        log.info('Shutting done')
        job_monitor.terminate()
        job_submitter.terminate()
        job_submitter.join()
        log.info('Clean up running jobs')

        database.connect()

        queued = ProcessingState.get(description='queued')
        running = ProcessingState.get(description='running')
        inserted = ProcessingState.get(description='inserted')

        for job in Job.select().where((Job.status == running) | (Job.status == queued)):
            sp.run(['scancel', '--jobname=erna_{}'.format(job.id)])
            job.status = inserted
            job.save()
        database.close()
