from threading import Thread, Event
import zmq
import logging

from .database import Job, ProcessingState, requires_database_connection

log = logging.getLogger(__name__)


class JobMonitor(Thread):

    def __init__(self, port=12700):

        super().__init__()

        self.event = Event()
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind('tcp://*:{}'.format(self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        log.info('JobMonitor running on port {}'.format(self.port))

    def run(self):
        while not self.event.is_set():

            events = self.poller.poll(timeout=1000)
            for socket, n_messages in events:
                for i in range(n_messages):
                    status_update = socket.recv_pyobj()
                    log.debug('Received status update: {}'.format(status_update))
                    self.update_job(status_update)
                    socket.send_pyobj(True)

    @requires_database_connection
    def update_job(status_update):
        job = Job.get(id=status_update['job_id'])
        status = status_update['status']
        job.status = ProcessingState.get(description=status)
        if status == 'success':
            job.result_file = status_update['output_file']
            job.md5hash = status_update['md5hash']
        job.save()

    def terminate(self):
        self.event.set()
