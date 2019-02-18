import subprocess
import os
import logging
import tempfile
from shutil import copyfile

from .utils import (
    assemble_facttools_call,
    check_environment_on_node
)
from .io import read_facttools_json


def run_facttools(job):
    '''
    This is what will be executed on the cluster
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    with tempfile.TemporaryDirectory() as output_directory:
        input_path = os.path.join(output_directory, "input.json")

        if job.outputfile:
            name, ext = os.path.splitext(job.outputfile)
            gzip = ext == '.gz'
            if gzip:
                name, ext = os.path.splitext(name)
        else:
            ext = '.json'

        tmp_output_path = os.path.join(output_directory, "output" + ext)

        job.run_df.to_json(input_path, orient='records', date_format='epoch')
        call = assemble_facttools_call(
            job.jar, job.xml, input_path, tmp_output_path, job.aux_path
        )

        check_environment_on_node()

        logger.info("Calling fact-tools with call: {}".format(call))
        try:
            subprocess.check_call(call)
        except subprocess.CalledProcessError as e:
            logger.error("Fact tools returned an error:")
            logger.error(e)
            return {'success': False, 'reason': str(e)}

        if not os.path.exists(tmp_output_path):
            return {'success': False, 'reason': 'No output file'}

        logger.info("Trying to collect output files")
        if job.outputfile:
            if gzip:
                try:
                    subprocess.check_call(["gzip", tmp_output_path])
                    tmp_output_path += '.gz'
                except subprocess.CalledProcessError as e:
                    logger.exception("Unable to gzip: {}".format(tmp_output_path))
                    return {'success': False, 'reason': 'gzip failed: ' + str(e)}

            logger.info("Copying zipped output file {}".format(tmp_output_path))
            copyfile(tmp_output_path, job.outputfile)
            return {'success': True, 'outputfile': job.outputfile}

        # try to read nans else return empty frame
        try:
            return {'success': True, 'events': read_facttools_json(tmp_output_path)}
        except ValueError:
            logger.exception("Fact-tools output could not be read.")
            return {'success': False, 'reason': 'error reading json'}
        except Exception as e:
            logger.exception("Fact-tools output could not be gathered.")
            return {'success': False, 'reason': 'error reading json: {}'.format(str(e))}
