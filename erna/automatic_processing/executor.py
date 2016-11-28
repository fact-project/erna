import subprocess as sp
import os
import logging
import tempfile
import sys
import shutil
from glob import iglob


log = logging.getLogger('erna')
log.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


def main():
    log.info('FACT Tools executor started')

    java = os.environ.get('JAVA_BIN', 'java')
    log.info('Using java executable: {}'.format(java))

    jar = os.environ['JARFILE']
    log.info('Using jar: {}'.format(jar))

    xml = os.environ['XMLFILE']
    log.info('Using xml: {}'.format(xml))

    output_path = os.environ['OUTPUTPATH']

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
                call.append('-D{}={}'.format(k.lstrip('facttools_'), v))

        sp.check_call(['which', java])
        sp.check_call(['free', '-m'])
        sp.check_call([java, '-Xmx512m', '-version'])

        log.info('Calling fact-tools with call: {}'.format(call))
        try:
            sp.check_call(call)
        except sp.CalledProcessError:
            log.exception('Fact tools returned an error:')
            sys.exit(1)

        for output_file in iglob(facttools_output):
            log.info('Copying {} to {}'.format(output_file, output_path))
            shutil.copyfile(output_file, output_path)

if __name__ == '__main__':
    main()
