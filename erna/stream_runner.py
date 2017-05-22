import subprocess
import pandas as pd
import os
import json
import logging
import tempfile
from erna import ft_json_to_df
from erna.utils import (
    assamble_facttools_call,
    check_environment_on_node,
    generate_paths_on_node
    )


def run(jar, xml, df, num, db_path=None):
    '''
    This is what will be executed on the cluster
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
        input_path, output_path = generate_paths_on_node(name, num, output_directory)

        df.to_json(input_path, orient='records', date_format='epoch')
        call = assamble_facttools_call(jar, xml, input_path, output_path, db_path)

        check_environment_on_node()

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

        #try to read nans else return empty frame
        return ft_json_to_df(output_path)
