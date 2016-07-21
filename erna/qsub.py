import numpy as np
import os
import pandas as pd
import subprocess
import logging
logger = logging.getLogger(__name__)

def get_qstat_as_df():
    """Get the current users output of qstat as a DataFrame.
    """
    user = os.environ.get("USER")
    try:
        ret = subprocess.Popen(["qstat", "-u", str(user)], stdout=subprocess.PIPE)
        df = pd.read_csv(ret.stdout, delimiter="\s+")
        # drop the first line since it is just one long line
        df = df.drop(df.index[0]).copy()
        # convert objects to numeric otherwise numbers are strings
        df["JOBID"] = pd.to_numeric(df["job-ID"], errors='coerce')
        # df.set_index("JOBID")
        df = df.drop('job-ID', 1)

    except ValueError:
        logger.exception("No jobs in queues for user {}".format(user))
        df = pd.DataFrame()
    return df


def get_finished_jobs(job_ids):
    """Get a list of finished job ids for the given list of jobs

    Keyword arguments:

    job_ids     -- list of lobs that shall be checked
    """
    data = get_qstat_as_df()
    finished_jobs = []

    if len(data) == 0:
        return job_ids

    ids_in_data = data[data["JOBID"].isin(job_ids)]
    finished_jobs = np.setdiff1d(job_ids, ids_in_data["JOBID"])

    return np.array(finished_jobs)


def get_running_jobs(job_ids=None, queue=None):
    """Get a DataFrame with the qstat output of running jobs of the current user.
    optionally a certain list of job ids and/or a certain queue can be given

    Keyword arguments:

    job_ids     -- list of lobs that shall be checked
    queue       -- name of the queue
    """
    data = get_qstat_as_df()
    if len(data) == 0:
        return data
    if queue is not None:
        data = data[data["queue"].str.contains(str(queue))]
    if job_ids is not None:
        data = data[data["JOBID"].isin(job_ids)]
    return data[data["state"] == "r"]


def get_pending_jobs(job_ids=None):
    """Get a DataFrame with the qstat output of pending jobs of the current user.
    optionally a certain list of job ids and/or a certain queue can be given

    Keyword arguments:

    job_ids     -- list of lobs that shall be checked
    queue       -- name of the queue
    """
    data = get_qstat_as_df()
    if len(data) == 0:
        return data
    #     data = data[data["queue"].str.contains(str(queue))].copy()
    if job_ids is not None:
        data = data[data["JOBID"].isin(job_ids)]
    return data[data["state"] == "qw"]
