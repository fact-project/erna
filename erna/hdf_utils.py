import logging

log = logging.getLogger(__name__)


def initialize_hdf5(f, dtypes, groupname='data', **kwargs):
    '''
    Create a group with name `groupname` and empty datasets for each
    entry in dtypes.

    Parameters
    ----------
    f: h5py.File
        the hdf5 file, opened either in write or append mode
    dtypes: numpy.dtype
        the numpy dtype object of a record or structured array describing
        the columns
    groupname: str
        the name for the hdf5 group to hold all datasets, default: data
    '''
    group = f.create_group(groupname)

    for name in dtypes.names:
        dtype = dtypes[name]
        maxshape = [None] + list(dtype.shape)
        shape = [0] + list(dtype.shape)

        group.create_dataset(
            name,
            shape=tuple(shape),
            maxshape=tuple(maxshape),
            dtype=dtype,
            **kwargs
        )

    return group


def append_to_hdf5(f, array, groupname='data'):
    '''
    Append a numpy record or structured array to the given hdf5 file
    The file should have been previously initialized with initialize_hdf5

    Parameters
    ----------
    f: h5py.File
        the hdf5 file, opened either in write or append mode
    array: numpy.array or numpy.recarray
        the numpy array to append
    groupname: str
        the name for the hdf5 group with the corresponding data sets
    '''

    group = f.get(groupname)

    for key in array.dtype.names:
        dataset = group.get(key)
        num_rows = dataset.shape[0]

        dataset.resize(num_rows + array.shape[0], axis=0)

        if len(dataset.shape) > 1:
            dataset[num_rows:, :] = array[key]
        else:
            dataset[num_rows:] = array[key]
