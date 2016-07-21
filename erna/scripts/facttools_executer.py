import subprocess
import os
import logging
import tempfile
import json
import pandas as pd
from erna import ft_json_to_df

logger = logging.getLogger(__name__)

def main():
    '''
    This is what will be executed on the cluster
    '''
    logger.info("facttools executor has been started.")

    input_path = os.environ.get("INPUTFILE")
    jar = os.environ.get("JARFILE")
    xml = os.environ.get("XMLFILE")
    output_path = os.environ.get("OUTPUTFILE")
    db_path = os.environ.get("DBPATH")

    hdf_output_path = output_path+".hdf"

    with tempfile.TemporaryDirectory() as output_directory:
        name, _ = os.path.splitext(os.path.basename(output_path))
        json_output_path = os.path.join(output_directory, "{}.json".format(name))

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
                '-Doutput=file:{}'.format(json_output_path),
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
            if os.path.exists(json_output_path):
                logger.error("Trying to collect output files")
            else:
                logger.error("fact-tools error")

        try:
            df_out = ft_json_to_df(json_output_path)
            df_out["fact_tools"] = os.path.basename(jar)
            df_out["xml"] = os.path.basename(xml)
            df_out.to_hdf(os.path.abspath(hdf_output_path), 'data', mode='w')
            os.remove(input_path)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            logger.exception("Fact-tools output could not be read.")

if __name__ == "__main__":
    main()
