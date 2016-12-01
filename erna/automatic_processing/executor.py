import subprocess as sp
import os
import logging
import tempfile
import sys
import shutil
from glob import iglob
import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)

log = logging.getLogger('erna')
log.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def main():
    log.info('FACT Tools executor started')

    host = os.environ['SUBMITTER_HOST']
    port = os.environ['SUBMITTER_PORT']
    socket.connect('tcp://{}:{}'.format(host, port))

    job_id = int(os.environ['JOB_NAME'].replace('erna_', ''))

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

    with tempfile.TemporaryDirectory() as tmp_dir:
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

        for k, v in os.environ.items():
            if k.startswith('facttools_'):
                option = '-D{}={}'.format(k.replace('facttools_', '', 1), v)
                call.append(option)

        sp.check_call(['which', java])
        sp.check_call(['free', '-m'])
        sp.check_call([java, '-Xmx512m', '-version'])

        log.info('Calling fact-tools with call: {}'.format(call))
        try:
            sp.check_call(call, cwd=tmp_dir)
        except sp.CalledProcessError:
            socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
            socket.recv()
            log.exception('Running FACT-Tools failed')
            sys.exit(1)

        try:
            output_file = next(iglob(os.path.join(facttools_output, '*')))
            log.info('Copying {} to {}'.format(output_file, output_dir))
            shutil.copy2(output_file, output_dir)
            log.info('Copy done')
        except:
            log.exception('Error copying outputfile')
            socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
            socket.recv()
            sys.exit(1)

    md5sum, _ = sp.check_output(['md5sum', output_file]).decode().split()

    socket.send_pyobj({
        'job_id': job_id,
        'status': 'success',
        'outputfile': output_file,
        'md5sum': md5sum,
    })
    socket.recv()

if __name__ == '__main__':
    main()
