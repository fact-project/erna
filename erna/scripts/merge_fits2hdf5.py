import click
import os

from ..hdf_utils import write_fits_to_hdf5

from fact.io import read_data, to_h5py


@click.command()
@click.argument('outputfile')
@click.argument(
    'inputfiles', nargs=-1,
    type=click.Path(exists=True, dir_okay=False)
)
@click.option(
    '-r', '--run-metadata',
    help='file with run metadata that is saved to the "runs" group of the outputfile',
)
def main(outputfile, inputfiles, run_metadata):
    '''
    Merge several fits files into HDF5 files using h5py.

    OUTPUTFILE: the outputfile
    INPUTFILE...: input fits files
    '''

    if os.path.isfile(outputfile):
        click.confirm(
            'Outputfile {} exists, overwrite?'.format(outputfile),
            abort=True,
        )

    write_fits_to_hdf5(outputfile, inputfiles, mode='w', key='events')

    if run_metadata:
        to_h5py(outputfile, read_data(run_metadata), key='runs', mode='a')
