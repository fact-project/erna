import logging
import click

import numpy as np
import sqlalchemy
import os

import time
import pandas as pd
import subprocess

import gridmap
from gridmap import Job

import erna
import erna.datacheck_conditions as dcc
from erna.qsub import *

from IPython import embed
logger = logging.getLogger(__name__)

def generate_qsub_command(name, queue, jar, xml, inputfile, outputfile, dbpath,
                          mail_address,mail_setting, stdout, stderr, engine, script):
    command_template = []
    command_template.append("qsub")
    command_template.append("-N {name}")
    command_template.append("-q {queue}")
    command_template.append("-M {mail_address}")
    command_template.append("-m {mail_setting}")
    command_template.append("-v {env}")
    if engine == "SGE":
        command_template.append("-b yes")
    command_template.append("-o {stdout}")
    command_template.append("-e {stderr}")
    command_template.append("{script}")

    command_template = " ".join(command_template)

    env = []
    env.append("INPUTFILE={0}".format(inputfile))
    env.append("JARFILE={0}".format(jar))
    env.append("XMLFILE={0}".format(xml))
    env.append("OUTPUTFILE={0}".format(outputfile))
    env.append("DBPATH={0}".format(dbpath))
    env.append("JOBNAME={0}".format(name))
    env.append("PATH={0}".format(os.environ.get("PATH")))
    env = ",".join(env)

    settings = {
        "name": name,
        "queue": queue,
        "mail_address": mail_address,
        "mail_setting": mail_setting,
        "env": env,
        "stdout": stdout,
        "stderr": stderr,
        "script": script,
    }

    return command_template.format(**settings)

def submit_qsub_jobs(jobname, jar, xml, db_path, df_mapping,  engine, queue, vmem, num_jobs_per_bunch, walltime, dbpath, mail_setting):
    jobs = []
    # create job objects
    # split_indices = np.array_split(np.arange(len(df_mapping)), num_jobs)
    df_mapping["bunch_index"]= np.arange(len(df_mapping)) // num_jobs_per_bunch

    tempfolder = os.environ.get('DEFAULT_TEMP_DIR')
    mail_address = os.environ.get('ERROR_MAIL_RECIPIENT')

    jobname = "_".join(jobname.split())

    for num, df in df_mapping.groupby("bunch_index"):
        df=df.copy()
        job_name = "{}_{}".format(jobname, num)

        stdout_file = os.path.join(tempfolder, job_name+".o")
        stderr_file = os.path.join(tempfolder, job_name+".e")
        input_path = os.path.join(tempfolder, job_name+".json")
        output_path = os.path.join(tempfolder, job_name+"_out.json")

        df.to_json(input_path, orient='records', date_format='epoch' )

        script_folder = os.path.dirname(os.path.realpath(__file__))

        command = generate_qsub_command(name=job_name, queue=queue, jar=jar, xml=xml, inputfile=input_path,
                                    outputfile=output_path, dbpath=dbpath, mail_address = mail_address, mail_setting=mail_setting, stdout=stdout_file, stderr=stderr_file, engine=engine,
                                    # script=os.path.join(script_folder, "facttools_executer.py"))
                                    script="facttools_executer")
        print(command)
        return_code = subprocess.check_output(command, shell=True)
        print(return_code)

        if engine == "SGE":
            df["JOBID"] = int(str(return_code.decode()).split(" ")[2])
        if engine == "PBS":
            df["JOBID"] = int(str(return_code.decode()).split(".")[0])

        df["output_path"] = output_path
        df["bunch_index"] = num
        df["command"] = command
        df["fact_tools"] = os.path.basename(jar)
        df["xml"] = os.path.basename(xml)
        jobs.append(df)

    return pd.concat(jobs, ignore_index=True)

@click.command()
@click.argument('earliest_night')
@click.argument('latest_night' )
@click.argument('data_dir', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True) )
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('db', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--mail', help='qsub mail settings.', default='a')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE')
@click.option('--num_runs', help='Number of runs per job to start on the cluster. (number of jobs will be calculated from that)', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--source',  help='Name of the source to analyze. e.g Crab', default='Crab')
@click.option('--conditions',  help='Name of the data conditions as given in datacheck_conditions.py e.g std', default='data')
@click.option('--max_delta_t', default=30,  help='Maximum time difference (minutes) allowed between drs and data files.', type=click.INT)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.password_option(help='password to read from the always awesome RunDB')
def main(earliest_night, latest_night, data_dir, jar, xml, db, out, queue, mail, walltime, engine, num_runs, vmem, log_level, port, source, conditions, max_delta_t, local, password):

    level=logging.INFO
    if log_level is 'DEBUG':
        level = logging.DEBUG
    elif log_level is 'WARN':
        level = logging.WARN
    elif log_level is 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(levelname)s - ' +  '%(message)s'), level=level)

    jarpath = os.path.abspath(jar)
    xmlpath =os. path.abspath(xml)
    outpath = os.path.abspath(out)
    erna.ensure_output(out)
    db_path = os.path.abspath(db)
    output_directory = os.path.dirname(outpath)
    #create dir if it doesnt exist
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Writing output data  to {}".format(out))
    factdb = sqlalchemy.create_engine("mysql+pymysql://factread:{}@129.194.168.95/factdata".format(password))
    data_conditions=dcc.conditions[conditions]
    df_runs = erna.load(earliest_night, latest_night, data_dir, source_name=source, timedelta_in_minutes=max_delta_t, factdb=factdb, data_conditions=data_conditions)
    logger.info("Processing {} jobs with {} runs per job.".format(int(len(df_runs)/num_runs), num_runs))

    click.confirm('Do you want to continue processing and start jobs?', abort=True)



    processing_identifier = "{}_{}".format(source, time.strftime('%Y%m%d%H%M'))
    df_runs = submit_qsub_jobs(processing_identifier, jarpath, xmlpath, db_path, df_runs,  engine, queue, vmem, num_runs, walltime, db, mail)

    jobids = df_runs["JOBID"].unique()
    nsubmited = len(jobids)
    nfinished = 0
    last_finished = []
    job_outputs = []

    while(nfinished < nsubmited):
        finished_jobs = get_finished_jobs(jobids)
        running_jobs = get_running_jobs(queue)
        pending_jobs = get_pending_jobs(queue)

        nfinished = len(finished_jobs)
        logger.info("Processing Status: running: {}, pending: {}, queued: {}, finished: {}/{}"
                    .format(len(running_jobs), len(pending_jobs), nsubmited-nfinished, nfinished, nsubmited))

        last_finished = np.setdiff1d(finished_jobs, last_finished)

        for jobid in last_finished:
            json_path = os.path.abspath(df.query("JOBID == 3318453").output_path.unique().item())
            logger.info("appending: {}".format(json_path))

            try:
                job_outputs.append(read_json(json_path))
                os.remove(json_path)
            except FileNotFoundError as e:
                logger.error("No Fact-tools output for: {}".format(json_path))

        time.sleep( 10*60 )
        logger.info("next qstat in 10 mins")

    erna.collect_output(job_outputs, out)
    df_runs.to_hdf(out, "jobinfo", mode="a")

if __name__ == "__main__":
    main()
