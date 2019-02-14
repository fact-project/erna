import os


def build_path(row, path_to_data, extension):
    """
    builds a path to the fact data given the night, extension and filename
    """
    night = str(row.night)
    year = night[0:4]
    month = night[4:6]
    day = night[6:8]
    res = os.path.join(path_to_data, year, month, day, row.filename + extension)
    return res


def test_drs_path(df, key):
    """
    Test if the given drs paths in the key are present
    """
    mask = df[key].apply(os.path.exists)
    df['drs_file_exists'] = mask

    return df


def test_data_path(df, key):
    """
    Test the given data paths in key if they exists. It tests for
    both possible fileextensions [.fz, .gz] and corrects if necessary.
    """
    mask = df[key].apply(os.path.exists)
    df['data_file_exists'] = mask
    df.loc[~mask, key] = df.loc[~mask, key].str.replace('.fz', '.gz')
    df.loc[~mask, 'data_file_exists'] = df.loc[~mask, key].apply(os.path.exists)

    return df


def build_filename(night, run_id):
    return night.astype(str) + '_' + run_id.map('{:03d}'.format)


def ensure_output(output_path):
    '''
    Make sure the output file does not exist yet.
    Create directorie to new output file if necessary
    '''
    if os.path.exists(output_path):
        raise FileExistsError('The output file already exists.')
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)



