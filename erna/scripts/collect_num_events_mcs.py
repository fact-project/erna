import click
from astropy.io import fits
from fact.io import write_data
from functools import partial
from multiprocessing import Pool, TimeoutError
from tqdm import tqdm
import numpy as np
import pandas as pd
import os

import logging

logLevel = dict()
logLevel["INFO"] = logging.INFO
logLevel["DEBUG"] = logging.DEBUG
logLevel["WARN"] = logging.WARN

renames = dict()
renames['NAXIS2'] = 'num_evens'

columns2read = ['LONS_RUNID', 'LONS_NIGHT', 'LONS_EventNum', 'EventNum', 'MCorsikaEvtHeader.fEvtNumber', 'MCorsikaRunHeader.fRunNumber']

@click.command()
@click.argument('infiles', nargs=-1, type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.argument('outfile', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True))
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
def main(infiles, outfile, log_level):
    """
    run over list of fits files and extract header information from them and store the result to hdf
    """

    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logLevel[log_level])
    log = logging.getLogger(__name__)

    log.info("Extracting num islands data")

    with Pool(48) as p:
        res_dict_list = []
        for res in tqdm(
                p.imap_unordered(
                    partial(extract_num_events_from_fits, keys=['NAXIS2','NAXIS2']),
                    infiles
                ),
                total=len(infiles)
        ):
            res_dict_list.append(res)
    df = pd.DataFrame(res_dict_list)

    write_data(df, outfile, key='num_events', mode='w')


def extract_num_events_from_fits(data_file, keys=columns2read):
    log = logging.getLogger(__name__)
    with fits.open(data_file) as hdu:
        res = dict()
        res["filename"] = os.path.realpath(data_file)

        for key in keys:
            keyname = key if not key in renames else renames[key]
            try:
                res[keyname] = hdu[1].data[key]
            except IndexError:
                log.warning(f"Couldn't read {key} HDU from fits file {data_file}.\n value will be nan!!!")
                res[keyname] = np.nan
        return pd.DataFrame(res)


if __name__ == '__main__':
    main()
