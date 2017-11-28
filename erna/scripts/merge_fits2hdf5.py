import click
import os

from ..hdf_utils import write_fits_to_hdf5


@click.command()
@click.argument('outputfile')
@click.argument(
    'inputfiles', nargs=-1,
    type=click.Path(exists=True, dir_okay=False)
)
@click.option(
    '-k', '--key', default='events', show_default=True,
    help='Output group in the hdf5 file',
)
def main(outputfile, inputfiles, key):
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

    write_fits_to_hdf5(outputfile, inputfiles, mode='w', key=key)
