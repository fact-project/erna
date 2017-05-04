import subprocess
import pandas as pd
import os
import json
import logging
import tempfile
from shutil import copyfile
from erna import ft_json_to_df

def run(jar, xml, df, num, output_dest, db_path=None):
    '''
    This is a version of ernas stream runner that will be executed on the cluster,
    but writes its results directly to disk without sending them
    via zeroMq
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")
    # if 'DEFAULT_TEMP_DIR' not in os.environ:
    #     logger.error("No scratch directory given via environment variable DEFAULT_TEMP_DIR. Aborting")
    #     return "No default temp dir"
    #
    # output_directory = os.environ['DEFAULT_TEMP_DIR']
    #
    # if not os.path.isdir(output_directory):
    #     logger.warn("Output directory {} does not exist. Trying to create it.".format(output_directory))
    #     try:
    #         os.mkdir(output_directory, mode=0o755)
    #     except OSError: # Python >2.5
    #             pass
    #     #check if that worked or not
    #     if not os.access(output_directory, os.W_OK | os.X_OK) :
    #         logger.error("Cannot write to directory given DEFAULT_TEMP_DIR {} ".format(output_directory))
    #         return "Cannot write to dir"


    with tempfile.TemporaryDirectory() as output_directory:
        name, _ = os.path.splitext(os.path.basename(xml))
        input_filename = "input_{}_{}.json".format(name ,num)
        output_filename = "output_{}_{}.json".format(name, num)

        input_path = os.path.join(output_directory, input_filename)
        output_path = os.path.join(output_directory, output_filename)

        # logger.info("Writing {} entries to json file  {}".format(len(df), filename))
        df.to_json(input_path, orient='records', date_format='epoch' )
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
                return "fact-tools error"

        copyfile(output_path, os.path.join(output_dest, output_filename))

        #try to read nans else return empty frame
        return ft_json_to_df(input_path)
