import subprocess
import os
import logging
import tempfile


def main():
    '''
    This is what will be executed on the cluster
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    input_path  = os.environ.get("INPUTFILE")
    jar         = os.environ.get("JARFILE")
    xml         = os.environ.get("XMLFILE")
    output_path = os.environ.get("OUTPUTFILE")
    db_path      = os.environ.get("DBPATH")
    jobname     = os.environ.get("JOBNAME")

    # logger.info("Writing {} entries to json file  {}".format(len(df), filename))
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
            '-Dinput=file:{}'.format(input_path),
            '-Doutput=file:{}'.format(output_path),
            '-Ddb=file:{}'.format(db_path),
    ]

    subprocess.check_call(['which', 'java'])
    subprocess.check_call(['free', '-m'])
    subprocess.check_call(['java', '-Xmx512m', '-version'])

    logger.info("Calling fact-tools with call: {}".format(call))
    try:
        subprocess.check_call(call)
    except subprocess.CalledProcessError as e:
        logger.error("Fact tools returned an error:")
        logger.error(e)
        if os.path.exists(output_path):
            logger.error("Trying to collect output files")
        else:
            logger.error("fact-tools error")

    os.remove(input_path)

if __name__ == "__main__":
    main()
