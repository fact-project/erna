import subprocess
import os
import logging
import tempfile
import json
import pandas as pd

def main():
    '''
    This is what will be executed on the cluster
    '''
    logger = logging.getLogger(__name__)
    logger.info("facttools executor has been started.")

    input_path = os.environ.get("INPUTFILE")
    jar = os.environ.get("JARFILE")
    xml = os.environ.get("XMLFILE")
    output_path = os.environ.get("OUTPUTFILE")
    db_path = os.environ.get("DBPATH")
    # jobname = os.environ.get("JOBNAME")

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

    # try to read nans else return empty frame
    with open(output_path,'r') as text:
        try:
            logger.info("Reading fact-tools output.")
            y=json.loads(text.read())
            df_out=pd.DataFrame(y)
            df_out["fact_tools"] = os.path.basename(jar)
            df_out["xml"] = os.path.basename(xml)

            logger.info("Saving data to hdf with {} entries".format(len(df_out)))
            name, extension = os.path.splitext(output_path)
            df_out.to_json(os.path.abspath(name+".hdf"), orient='records', date_format='epoch')
            os.remove(output_path)

        except ValueError as e:
            logger.error("Fact-tools output could not be read.")
            print(e)

        except Exception as e:
            print(e)
            logger.error("error gathering output")


if __name__ == "__main__":
    main()
