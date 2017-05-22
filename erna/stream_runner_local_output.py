import subprocess
import pandas as pd
import os
import json
import logging
import tempfile
from shutil import copyfile
from erna import ft_json_to_df
from erna.utils import (
    assamble_facttools_call,
    check_environment_on_node,
    generate_paths_on_node
    )


def run(jar, xml, df, num, output_dest, db_path=None):
    '''
    This is a version of ernas stream runner that will be executed on the cluster,
    but writes its results directly to disk without sending them
    via zeroMq
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    with tempfile.TemporaryDirectory() as output_directory:
        name, _ = os.path.splitext(os.path.basename(xml))
        input_path, tmp_output_path = generate_paths_on_node(name, num, output_directory)

        df.to_json(input_path, orient='records', date_format='epoch')
        call = assamble_facttools_call(jar, xml, input_path, tmp_output_path, db_path)

        check_environment_on_node()

        logger.info("Calling fact-tools with call: {}".format(call))
        try:
            subprocess.check_call(call)
        except subprocess.CalledProcessError as e:
            logger.error("Fact tools returned an error:")
            logger.error(e)
            return "fact-tools error"

        if not os.path.exists(tmp_output_path):
            logger.error("Not output generated, returning no results")
            return "fact-tools generated no output"

        #try to read nans else return empty frame
        return ft_json_to_df(input_path)
