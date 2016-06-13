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
import erna.qsub as q

from IPython import embed
logger = logging.getLogger(__name__)


def last_finished_out_paths(df_submitted, last_finished):
    output_paths = []

    for jobid in last_finished:
        row = df_submitted.query("JOBID == {}".format(jobid))
        for outpath in row.output_path.unique():
            hdf_path = os.path.abspath(outpath+".hdf")
            logger.info("appending: {}".format(hdf_path))
            output_paths.append(hdf_path)
    return output_paths


def read_outputs_to_list(job_output_paths):
    job_outputs = []
    for job_output_path in job_output_paths:
        try:
            df_out = pd.read_hdf(job_output_path, "data")
            job_outputs.append(df_out)
        except Exception as e:
            logger.error("{} could not be appended.".format(job_output_path))
            print(e)
    return job_outputs


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


def submit_qsub_jobs(jobname, jar, xml, db_path, df_mapping,  engine, queue,
                     vmem, num_jobs_per_bunch, walltime, dbpath, mail_setting):
    jobs = []
    # create job objects
    # split_indices = np.array_split(np.arange(len(df_mapping)), num_jobs)
    df_mapping["bunch_index"]= np.arange(len(df_mapping)) // num_jobs_per_bunch

    tempfolder = os.environ.get('DEFAULT_TEMP_DIR')
    mail_address = os.environ.get('ERROR_MAIL_RECIPIENT')

    jobname = "_".join(jobname.split())

    for num, group in df_mapping.groupby("bunch_index"):
        df_jobs=group.copy()
        job_name = "{}_{}".format(jobname, num)

        stdout_file = os.path.join(tempfolder, job_name+".o")
        stderr_file = os.path.join(tempfolder, job_name+".e")
        input_path = os.path.join(tempfolder, job_name+".json")
        output_path = os.path.join(tempfolder, job_name+"_out")

        group.to_json(input_path, orient='records', date_format='epoch')

        command = generate_qsub_command(name=job_name, queue=queue, jar=jar,
                                        xml=xml, inputfile=input_path,
                                        outputfile=output_path, dbpath=dbpath,
                                        mail_address=mail_address,
                                        mail_setting=mail_setting,
                                        stdout=stdout_file, stderr=stderr_file,
                                        engine=engine,
                                        script="facttools_executer")

        return_code = subprocess.check_output(command, shell=True)
        logger.info(return_code.decode())

        if engine == "SGE":
            df_jobs["JOBID"] = int(str(return_code.decode()).split(" ")[2])
        if engine == "PBS":
            df_jobs["JOBID"] = int(str(return_code.decode()).split(".")[0])


        df_jobs["output_path"] = output_path
        df_jobs["bunch_index"] = num
        df_jobs["command"] = command
        df_jobs["fact_tools"] = os.path.basename(jar)
        df_jobs["xml"] = os.path.basename(xml)
        jobs.append(df_jobs)
            # if num>=1:
            #     break
    if len(jobs) == 0:
        return pd.DataFrame()

    return pd.concat(jobs)


@click.command()
@click.argument('earliest_night')
@click.argument('latest_night')
@click.argument('data_dir', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
@click.argument('jar', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('xml', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('db', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True))
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--mail', help='qsub mail settings.', default='a')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='SGE')
@click.option('--num_runs', help='Number of runs per job to start on the cluster. (number of jobs will be calculated from that)', default='4', type=click.INT)
@click.option('--qjobs', help='Number of jobs to be qued on the cluster at the same time.', default='100', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=12856, type=int)
@click.option('--source',  help='Name of the source to analyze. e.g Crab', default='Crab')
@click.option('--conditions',  help='Name of the data conditions as given in datacheck_conditions.py e.g std', default='data')
@click.option('--max_delta_t', default=30,  help='Maximum time difference (minutes) allowed between drs and data files.', type=click.INT)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.password_option(help='password to read from the always awesome RunDB')
def main(earliest_night, latest_night, data_dir, jar, xml, db, out, queue, mail,
         walltime, engine, num_runs, qjobs, vmem, log_level, port, source, conditions,
         max_delta_t, local, password):

    level=logging.INFO
    if log_level is 'DEBUG':
        level = logging.DEBUG
    elif log_level is 'WARN':
        level = logging.WARN
    elif log_level is 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(levelname)s - ' + '%(message)s'), level=level)

    jarpath = os.path.abspath(jar)
    xmlpath =os. path.abspath(xml)
    outpath = os.path.abspath(out)
    erna.ensure_output(out)
    logger.info("Output data will be written to {}".format(out))

    db_path = os.path.abspath(db)
    output_directory = os.path.dirname(outpath)
    # create dir if it doesnt exist
    os.makedirs(output_directory, exist_ok=True)

    factdb = sqlalchemy.create_engine("mysql+pymysql://factread:{}@129.194.168.95/factdata".format(password))
    data_conditions=dcc.conditions[conditions]
    df_loaded = erna.load(earliest_night, latest_night, data_dir, source_name=source, timedelta_in_minutes=max_delta_t, factdb=factdb, data_conditions=data_conditions)
    df_loaded.to_hdf(out+".tmp", "loaded", mode="a")

    logger.info("Processing {} jobs with {} runs per job.".format(int(len(df_loaded)/num_runs), num_runs))
    click.confirm('Do you want to continue processing and start jobs?', abort=True)

    #ensure that the max number of queuable jobs is smaller than the total number of jobs
    if qjobs > len(df_loaded):
        qjobs = len(df_loaded)

    nfinished = 0
    nsubmited = 1
    running_jobs = []
    pending_jobs = []
    last_finished = []
    jobids = []
    job_output_paths = []
    df_submitted = pd.DataFrame()

    #copy then dataframe with loaded jobs to submit
    df_runs = df_loaded.copy()

    #operate submission loop, as long as jobs need to be submitted
    while(nfinished < nsubmited):
        n_toqueue = qjobs - (len(pending_jobs) + len(running_jobs))
        logger.info("{} jobs to be queued".format(n_toqueue))

        if ( n_toqueue > 0 ) and ( len(df_runs) > 0):
            df_to_submit = df_runs.head(n_toqueue*num_runs).copy()
            processing_identifier = "{}_{}".format(source, time.strftime('%Y%m%d%H%M'))
            df_submitted_last = submit_qsub_jobs(processing_identifier, jarpath, xmlpath, db_path, df_to_submit,  engine, queue, vmem, num_runs, walltime, db, mail)
            df_submitted = df_submitted.append(df_submitted_last)


            #append submitted jobids
            jobids = df_submitted["JOBID"].unique()
            df_runs = df_runs.drop(df_to_submit.index)
            nsubmited = len(jobids)
            logger.info("Submitted {} jobs in last bunch".format(len(df_submitted_last)))
            logger.info("Submitted {} jobs in total".format(nsubmited))


        finished_jobs = q.get_finished_jobs(jobids)
        running_jobs = q.get_running_jobs(jobids)
        pending_jobs = q.get_pending_jobs(jobids)

        nfinished = len(finished_jobs)
        logger.info("Processing Status: running: {}, pending: {}, queued: {}, finished: {}/{}"
                    .format(len(running_jobs), len(pending_jobs),
                        nsubmited-nfinished, nfinished, nsubmited))

        last_finished = np.setdiff1d(finished_jobs, last_finished)

        if len(last_finished) > 0:
            last_paths = last_finished_out_paths(df_submitted, last_finished)
            job_output_paths = np.append(job_output_paths, last_paths)

        last_finished = finished_jobs
        if nfinished < nsubmited:
            logger.info("Checking qstat in 5 min again")
            time.sleep(5*60)

    logger.info("All jobs have been finished, processing done")

    job_outputs = read_outputs_to_list(job_output_paths)
    erna.collect_output(job_outputs, out, df_started_runs=df_loaded)
    # erna.collect_output(job_output_paths, out)
    df_loaded.to_hdf(out, "loaded", mode="a")
    df_submitted.to_hdf(out, "jobinfo", mode="a")
    os.remove(out+".tmp")

if __name__ == "__main__":
    main()
