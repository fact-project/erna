import time
start_time = time.monotonic()

import subprocess as sp
import os
import logging
import tempfile
import sys
import shutil
from glob import iglob
import zmq
import stat

from ..utils import chown

context = zmq.Context()
socket = context.socket(zmq.REQ)

log = logging.getLogger('erna')
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
fmt = logging.Formatter(fmt='%(asctime)s [%(levelname)-8s] %(message)s')
handler.setFormatter(fmt)
logging.getLogger().addHandler(handler)


def main():
    log.info('FACT Tools executor started')

    host = os.environ['SUBMITTER_HOST']
    port = os.environ['SUBMITTER_PORT']
    socket.connect('tcp://{}:{}'.format(host, port))

    job_id = int(os.environ['SLURM_JOB_NAME'].replace('erna_', ''))

    socket.send_pyobj({'job_id': job_id, 'status': 'running'})
    socket.recv()

    java = os.environ.get('JAVA_BIN', 'java')
    log.info('Using java executable: {}'.format(java))

    jar = os.path.abspath(os.environ['JARFILE'])
    log.info('Using jar: {}'.format(jar))

    xml = os.path.abspath(os.environ['XMLFILE'])
    log.info('Using xml: {}'.format(xml))

    output_dir = os.path.abspath(os.environ['OUTPUTDIR'])
    os.makedirs(output_dir, exist_ok=True)

    walltime = float(os.environ['WALLTIME'])
    log.info('Walltime = %.0f', walltime)

    fact_tools_options = {}
    for k, v in os.environ.items():
        if k.startswith('facttools_'):
            k = k.replace('facttools_', '', 1)
            fact_tools_options[k] = v

    for k in ['infile', 'drsfile']:
        f = fact_tools_options[k].replace('file:', '', 1)
        if not os.path.isfile(f):
            socket.send_pyobj({'job_id': job_id, 'status': 'input_file_missing'})
            socket.recv()
            log.exception('Missing input file {}'.format(f))
            sys.exit(1)

    job_name = 'fact_erna_job_id_' + str(job_id) + '_'
    with tempfile.TemporaryDirectory(prefix=job_name) as tmp_dir:
        log.debug('Using tmp directory: {}'.format(tmp_dir))
        facttools_output = os.path.join(tmp_dir, 'facttools_output')
        os.makedirs(facttools_output)

        call = [
            'java',
            '-XX:MaxHeapSize=1024m',
            '-XX:InitialHeapSize=512m',
            '-XX:CompressedClassSpaceSize=64m',
            '-XX:MaxMetaspaceSize=128m',
            '-XX:+UseConcMarkSweepGC',
            '-XX:+UseParNewGC',
            '-jar',
            jar,
            xml,
        ]

        for k, v in fact_tools_options.items():
            call.append('-D{}={}'.format(k, v))

        try:
            sp.run(['which', java], check=True)
            sp.run(['free', '-m'], check=True)
            sp.run([java, '-Xmx512m', '-version'], check=True)

            log.info('Calling fact-tools with call: "{}"'.format(' '.join(call)))
            timeout = walltime - (time.monotonic() - start_time) - 300
            log.info('Setting fact-tools timout to %.0f', timeout)
            sp.run(call, cwd=tmp_dir, check=True, timeout=timeout)
        except sp.CalledProcessError:
            socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
            socket.recv()
            log.exception('Running FACT-Tools failed')
            sys.exit(1)
        except sp.TimeoutExpired:
            socket.send_pyobj({'job_id': job_id, 'status': 'walltime_exceeded'})
            log.error('FACT Tools about to run into wall-time, terminating')
            socket.recv()
            sys.exit(1)

        try:
            tmp_output = next(iglob(os.path.join(facttools_output, '*')))
            base = os.path.basename(tmp_output)
            output_file = os.path.join(output_dir, base + '.gz')
            log.info('Gzipping {} to {}'.format(tmp_output, output_file))
            with open(output_file, 'wb') as f:
                sp.run(['gzip', '-c', tmp_output], stdout=f)

            log.info('gzipping done')
        except:
            log.exception('Error gzipping outputfile')
            socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
            socket.recv()
            sys.exit(1)

    try:
        process = sp.run(['md5sum', output_file], check=True, stdout=sp.PIPE)
        md5hash, _ = process.stdout.decode().split()
    except:
        log.exception('Error calculating md5sum')
        socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
        socket.recv()
        sys.exit(1)

    try:
        groupname = os.environ.get('ERNA_GROUP', None)
        chown(output_file, username=None, groupname=groupname)
        os.chmod(output_file, stat.S_IWUSR | stat.S_IRUSR | stat.S_IWGRP | stat.S_IRGRP)
    except OSError:
        log.exception('Error setting file permissions')
        socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
        socket.recv()
        sys.exit(1)

    socket.send_pyobj({
        'job_id': job_id,
        'status': 'success',
        'output_file': output_file,
        'md5hash': md5hash,
    })
    socket.recv()

if __name__ == '__main__':
    main()
