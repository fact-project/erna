import logging
import h5py
from astropy.io import fits
from tqdm import tqdm
import sys
from fact.io import append_to_h5py, initialize_h5py
from fact.instrument import camera_distance_mm_to_deg
from numpy.lib import recfunctions
import numpy as np
from .features import rename_columns

log = logging.getLogger(__name__)

native_byteorder = {'little': '<', 'big': '>'}[sys.byteorder]


theta_columns = tuple(
    ['theta'] + ['theta_off_{}'.format(i) for i in range(1, 6)]
)

theta_deg_columns = tuple(
    ['theta_deg'] + ['theta_deg_off_{}'.format(i) for i in range(1, 6)]
)


def write_fits_to_hdf5(
        outputfile,
        inputfiles,
        mode='a',
        compression='gzip',
        progress=True,
        key='events'):

    initialized = False

    version = None
    with h5py.File(outputfile, mode) as hdf_file:

        for inputfile in tqdm(inputfiles, disable=not progress):
            with fits.open(inputfile) as f:

                if version is None:
                    version = f[0].header['VERSION']
                    hdf_file.attrs['fact_tools_version'] = version
                else:
                    if version != f[0].header['VERSION']:
                        raise ValueError(
                            'Merging output of different FACT-Tools versions not allowed'
                        )

                if len(f) < 2:
                    continue

                array = np.array(f[1].data[:])

                # convert all names to snake case
                array.dtype.names = rename_columns(array.dtype.names)

                # add columns with theta in degrees
                arrays = []
                names = []
                for in_col, out_col in zip(theta_columns, theta_deg_columns):
                    if in_col in array.dtype.names:
                        arrays.append(camera_distance_mm_to_deg(array[in_col]))
                        names.append(out_col)

                if len(names) > 0:
                    array = recfunctions.append_fields(
                        array,
                        names=names,
                        data=arrays,
                        usemask=False,
                    )

                if not initialized:
                    initialize_h5py(
                        hdf_file,
                        array,
                        key=key,
                        compression=compression,
                    )
                    initialized = True

                append_to_h5py(hdf_file, array, key=key)

            if 'timestamp' in array.dtype.names:
                hdf_file[key]['timestamp'].attrs['timeformat'] = 'iso'
