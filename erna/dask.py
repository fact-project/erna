from dask_jobqueue import SLURMCluster, SGECluster, PBSCluster
from dask.distributed import Client
from .run_facttools import run_facttools

clusters = {
    'SLURM': SLURMCluster,
    'SGE': SGECluster,
    'PBS': PBSCluster,
}


class Cluster:
    def __init__(self, n_jobs, engine='SLURM', **kwargs,):
        self.cluster = clusters[engine](
            processes=1,
            cores=1,
            **kwargs,
        )
        self.n_jobs = n_jobs
        self.client = Client(self.cluster)

    def process_jobs(self, job_list):
        return self.client.map(run_facttools, job_list)

    def __enter__(self):
        self.cluster.start_workers(self.n_jobs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cluster.stop_all_jobs()
        self.client.close()
        self.cluster.close()
