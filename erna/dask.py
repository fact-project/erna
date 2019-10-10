from dask_jobqueue import SLURMCluster, SGECluster, PBSCluster
from dask.distributed import Client, LocalCluster
from .run_facttools import run_facttools

clusters = {
    'slurm': SLURMCluster,
    'sge': SGECluster,
    'pbs': PBSCluster,
}


class Cluster:
    def __init__(self, n_jobs, engine='SLURM', **kwargs,):
        if engine.lower() != 'local':
            try:
                self.cluster = clusters[engine.lower()](
                    processes=1,
                    cores=1,
                    **kwargs,
                )
            except KeyError:
                raise ValueError('Unsupported cluster engine')

        else:
            self.cluster = LocalCluster(n_workers=n_jobs, threads_per_worker=1)

        self.engine = engine
        self.n_jobs = n_jobs
        self.client = Client(self.cluster)

    def process_jobs(self, job_list):
        return self.client.map(run_facttools, job_list)

    def __enter__(self):
        if not isinstance(self.cluster, LocalCluster):
            self.cluster.start_workers(self.n_jobs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not isinstance(self.cluster, LocalCluster):
            self.cluster.stop_all_jobs()
        self.client.close()
        self.cluster.close()
