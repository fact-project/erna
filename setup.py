from setuptools import setup

setup(
    name='erna',
    version='0.0.2',
    description='Easy RuN Access. Tools that help to do batch processing of FACT data',
    url='https://github.com/fact-project/erna',
    author='Kai Brügge',
    author_email='kai.bruegge@tu-dortmund.de',
    license='BEER',
    packages=[
        'erna',
    ],
    # dependency_links = ['git+https://github.com/mackaiver/gridmap.git#egg=gridmap'],
    install_requires=[
        'pandas',           # in anaconda
        'numpy',            # in anaconda
        'matplotlib>=1.4',  # in anaconda
        'python-dateutil',  # in anaconda
        'sqlalchemy',       # in anaconda
        'PyMySQL',          # in anaconda
        'pytz',             # in anaconda
        'tables',
        # 'hdf5',
        'click',
        'drmaa',
        'pyzmq',
        'numexpr',
        # 'gridmap>=0.13.1',
    ],
   zip_safe=False,
   entry_points={
    'console_scripts': [
        'process_fact_data = scripts.process_fact_data',
        'process_fact_mc = scripts.process_fact_mc',
        'fetch_fact_runs = scripts.fetch_fact_runs',
        'process_fact_run_list = scripts.process_fact_run_list',
    ],
  }
)
