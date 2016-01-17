from distutils.core import setup

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
    install_requires=[
        'pandas',           # in anaconda
        'numpy',            # in anaconda
        'matplotlib>=1.4',  # in anaconda
        'python-dateutil',  # in anaconda
        'sqlalchemy',       # in anaconda
        'PyMySQL',          # in anaconda
        'pytz',             # in anaconda
        'click',
        'drmaa',
        'pyzmq',
        'numexpr',
    ],
    # scripts=['scripts/shift_helper', 'scripts/qla_bot'],
    # package_data={'fact_shift_helper.tools': ['config.gpg']},
    zip_safe=False
)
