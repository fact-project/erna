from setuptools import setup

setup(
    name='erna',
    version='0.0.1',
    description='Easy RuN Access. Tools that help batch processing of fact data',
    url='https://github.com/mackaiver/erna',
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
   zip_safe=False
)
