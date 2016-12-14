from .utils import load_config
import pandas as pd


default_columns = (
    'fNight AS night',
    'fRunID AS run_id',
    'fSourceName AS source',
    'fOnTime AS ontime',
    'fZenithDistanceMean AS zenith',
)


query_template = '''
SELECT {columns}
FROM RunInfo
JOIN Source
ON RunInfo.fSourceKey = Source.fSourceKey
JOIN RunType
ON RunType.fRunTypeKey = RunType.fRunTypeKey
WHERE {conditions}
;
'''


def get_runs(engine, conditions=None, columns=default_columns):

    if conditions is None:
        conditions = '1'
    else:
        conditions = ' AND '.join(conditions)

    query = query_template.format(
        columns=','.join(columns),
        conditions=conditions
    )

    return pd.read_sql_query(query, engine).set_index(['night', 'run_id'])
