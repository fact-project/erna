from fact.instrument import camera_distance_mm_to_deg
import re


snake_re_1 = re.compile('(.)([A-Z][a-z]+)')
snake_re_2 = re.compile('([a-z0-9])([A-Z])')


renames = {'RUNID': 'run_id', 'COGx': 'cog_x', 'COGy': 'cog_y'}


def camel2snake(key):
    ''' see http://stackoverflow.com/a/1176023/3838691 '''
    s1 = snake_re_1.sub(r'\1_\2', key)
    s2 = snake_re_2.sub(r'\1_\2', s1).lower().replace('__', '_')
    s3 = re.sub('^m_', '', s2)
    return s3.replace('.f_', '_')


def rename_columns(columns):
    return [camel2snake(renames.get(col, col)) for col in columns]


def add_theta_deg_columns(df):
    for i in range(6):
        incol = 'theta' if i == 0 else 'theta_off_{}'.format(i)
        outcol = 'theta_deg' if i == 0 else 'theta_deg_off_{}'.format(i)
        if incol in df.columns:
            df[outcol] = camera_distance_mm_to_deg(df[incol])
