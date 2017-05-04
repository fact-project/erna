import logging
import h5py
from astropy.io import fits
from tqdm import tqdm
import sys
from fact.io import append_to_h5py, initialize_h5py

log = logging.getLogger(__name__)

native_byteorder = {'little': '<', 'big': '>'}[sys.byteorder]


def write_fits_to_hdf5(
        outputfile,
        inputfiles,
        mode='a',
        compression='gzip',
        progress=True,
        groupname='events',
        ):

    initialized = False

    with h5py.File(outputfile, mode) as hdf_file:

        for inputfile in tqdm(inputfiles, disable=not progress):
            with fits.open(inputfile) as f:
                if len(f) < 2:
                    continue

                if not initialized:
                    initialize_h5py(
                        hdf_file,
                        f[1].data.dtype,
                        groupname=groupname,
                        compression=compression,
                    )
                    initialized = True

                append_to_h5py(hdf_file, f[1].data, groupname=groupname)
