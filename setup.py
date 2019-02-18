from setuptools import setup

setup(
    name='erna',
    version='0.9.0',
    description='Easy RuN Access. Tools that help to do batch processing of FACT data',
    url='https://github.com/fact-project/erna',
    author='Kai Brügge, Jens Buss, Maximilian Nöthe',
    author_email='kai.bruegge@tu-dortmund.de',
    license='BEER',
    packages=[
        'erna',
        'erna.scripts',
        'erna.automatic_processing',
    ],
    package_data={
        'erna': ['resources/*'],
        'erna.automatic_processing': ['xml/*']
    },
    install_requires=[
        'pandas',           # in anaconda
        'numpy',            # in anaconda
        'matplotlib>=1.4',  # in anaconda
        'python-dateutil',  # in anaconda
        'sqlalchemy',       # in anaconda
        'pymysql',          # in anaconda
        'pytz',             # in anaconda
        'tables',
        'pyfact>=0.22.1',
        'astropy',
        'h5py',
        'dask-jobqueue',
        'distributed',
        'tqdm',
        'click',
        'pyzmq',
        'peewee~=3.0',
        'numexpr',
        'pyyaml',
        'pytest',           # also in  conda
        'xmltodict',
        'wrapt',
        'retrying',
        # 'fact_condition', install from https://github.com/fact-project/fact_conditions
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'erna_process_data = erna.scripts.process_fact_data:main',
            'erna_process_mc = erna.scripts.process_fact_mc:main',
            'process_fact_run_list = erna.scripts.process_fact_run_list:main',
            'fetch_fact_runs = erna.scripts.fetch_fact_runs:main',
            'read_aux_files_to_sqlite = erna.scripts.read_aux_files_to_sqlite:main',
            'facttools_executer = erna.scripts.facttools_executer:main',
            'erna_fill_database = erna.scripts.fill_database:main',
            'erna_check_availability = erna.scripts.check_availability:main',
            'erna_create_tables = erna.scripts.create_db:main',
            'erna_upload = erna.scripts.upload:main',
            'erna_console = erna.scripts.console:main',
            'erna_automatic_processing_executor = erna.automatic_processing.executor:main',
            'erna_automatic_processing = erna.automatic_processing.__main__:main',
            'erna_gather_fits = erna.scripts.gather_fits:main',
            'erna_submit_runlist = erna.scripts.submit_runlist:main',
        ],
    },
    setup_requires=['pytest-runner'],
    tests_require=['pytest>=3.0.0'],
)
