import click
import os

from ..hdf_utils import write_fits_to_hdf5


@click.command()
@click.argument('outputfile')
@click.argument(
    'inputfiles', nargs=-1,
    type=click.Path(exists=True, dir_okay=False)
)
def main(outputfile, inputfiles):
    '''
    Gather the fits outputfiles of the erna automatic processing into a hdf5 file.
    The hdf5 file is written using h5py and contains the level 2 features in the
    `events` group and some metadata for each run in the `runs` group.

    It is possible to only gather files that pass a given datacheck with the --datacheck
    option. The possible conditions are implemented in erna.datacheck_conditions/

    XML_NAME: name of the xml for which you want to gather output
    FT_VERSION: FACT Tools version for which you want to gather output
    OUTPUTFILE: the outputfile
    '''

    if os.path.isfile(outputfile):
        click.confirm('Outputfile exists, overwrite?', abort=True)

    write_fits_to_hdf5(outputfile, inputfiles, mode='w')
