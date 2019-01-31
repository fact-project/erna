import subprocess
import pandas as pd
import os
import json

import tempfile
import atexit
import numpy as np
from astropy.io import fits
import logging

from erna import ft_json_to_df
from erna.utils import (
    assemble_facttools_call,
    check_environment_on_node,
    read_ids_from_fits
    )


def run(jar, xml, input_files_df, aux_source_path=None):
    '''
    This is what will be executed on the cluster
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")
    
    tempdir = os.getenv("LOCAL_TEMP_DIR", default=None)
    
    if tempdir:
        os.makedirs(tempdir, exist_ok=True)
        
    with tempfile.TemporaryDirectory(dir=tempdir) as output_directory:
        logger.info("Writing temporarily to {}".format(output_directory))
        
        @atexit.register
        def exit_handler():
            logger.info("removing tempdir")
            try:
                os.removedirs(output_directory)
            except FileNotFoundError:
                logger.debug("tempdir has allready been deleted")
            
        input_path = os.path.join(output_directory, "input.json")
        output_path = os.path.join(output_directory, "output.json")
        
        logger.info("Input files: {}".format(", ".join(input_files_df["data_path"])))

        input_files_df.to_json(input_path, orient='records', date_format='epoch')
        call = assemble_facttools_call(jar, xml, input_path, output_path, aux_source_path)

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

        event_ids = []
        for data_path in input_files_df['data_path'].values:
            event_ids.append(read_ids_from_fits(data_path))

        event_ids = pd.concat(event_ids)

        df_out = ft_json_to_df(output_path)

        id_columns = list(event_ids.keys())
        id_columns.remove('data_path')

        df_out = pd.merge(df_out, event_ids, how='left', on=id_columns, validate='one_to_one')
        df_out = df_out.merge(input_files_df, how='left', on='data_path')
        # try to read nans else return empty frame
        return df_out
