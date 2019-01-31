import subprocess
import pandas as pd
import os
import json
import logging
import tempfile
from shutil import copyfile
import atexit
from fact.io import write_data
from erna import ft_json_to_df
from erna.utils import (
    assemble_facttools_call,
    check_environment_on_node
    )


def run(jar, xml, input_files_df, output_path, aux_source_path=None):
    '''
    This is a version of ernas stream runner that will be executed on the cluster,
    but writes its results directly to disk without sending them
    via zeroMq
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")
    
    tempdir = os.getenv("LOCAL_TEMP_DIR", default=None)
    
    if tempdir:
        os.makedirs(tempdir, exist_ok=True)

    with tempfile.TemporaryDirectory(dir = tempdir) as output_directory:
        logger.info("Writing temporarily to {}".format(output_directory))
        
        @atexit.register
        def exit_handler():
            logger.info("removing tempdir")
            try:
                os.removedirs(output_directory)
            except FileNotFoundError:
                logger.debug("tempdir has allready been deleted")
            
        input_path = os.path.join(output_directory, "input.json")
        tmp_output_path = os.path.join(output_directory, "output.json")
        
        logger.info("Input files: {}".format(", ".join(input_files_df["data_path"])))

        input_files_df.to_json(input_path, orient='records', date_format='epoch')
        call = assemble_facttools_call(jar, xml, input_path, tmp_output_path, aux_source_path)
        logger.info(call)
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
        
        pre, out_ext = os.path.splitext(output_path)
        
        if out_ext == ".gz":
            try:
                subprocess.check_call(["gzip", tmp_output_path])
            except subprocess.CalledProcessError as e:
                logger.exception("Unable to zip: {}".format(tmp_output_path))

            tmp_output_path += out_ext
            logger.info("Copying zipped output file {} to {}".format(tmp_output_path, output_path))
        elif (out_ext == ".hdf5") or (out_ext == ".hdf") or (out_ext == ".h5"):
            df = ft_json_to_df(tmp_output_path)
            pre, ext = os.path.splitext(tmp_output_path)
            tmp_output_path = pre + out_ext
            if len(df) > 0:
                write_data(df, tmp_output_path, key="erna")
            else:
                logger.error('No events where returned')

        # create subfolder to hold the runs
        dirname = os.path.dirname(os.path.abspath(output_path))
        filename = os.path.basename(output_path)
        subfolder = "_".join(filename.split("_")[:-1])
        subfolder = os.path.join(dirname, subfolder)
        os.makedirs(subfolder, exist_ok=True)
        output_path = os.path.join(subfolder, filename)

        copyfile(tmp_output_path, output_path)
        
        input_files_df['output_path'] = output_path

        return input_files_df
