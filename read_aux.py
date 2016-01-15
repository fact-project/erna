from astropy.io import fits
# from IPython import embed
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import glob
import os
import click
from tqdm import *

@click.command()
@click.argument('aux_folder', type=click.Path(file_okay=False, dir_okay=True, exists=True))
@click.argument('sqlite_file', type=click.Path(file_okay=True, dir_okay=False,exists=False))
@click.option('--chunksize', default=10000,  help='Size of the chunks that will be written to the database.')
@click.option('--replace', 'behaviour', flag_value='replace', default=True)
@click.option('--append', 'behaviour', flag_value='append', default=False)
def main(aux_folder, sqlite_file, chunksize, behaviour):
    ''' This tool creates a sqlite database from the important values in the DRIVE_CONTROL_TRACKING and SOURCE aux files.
        The path given by the aux_folder argument will be searched recursively for file matching the usual filename conventions.
        That is: *DRIVE_CONTROL_TRACKING_POSITION.fits" and so on. This database may become huge.
        This script also eats alot of memory and runtime. Because I'm lazy.
        The DB created by this script can be read by the SqliteService in the fact-tools project.
    '''
    engine = create_engine('sqlite:///'+ sqlite_file, convert_unicode=True)
    print("Loading files")

    # pointing_files = glob.glob(aux_folder + "/**/*DRIVE_CONTROL_POINTING_POSITION.fits", recursive=True)
    # print("Found {} pointing position files in path {}".format(len(pointing_files), aux_folder))
    # df_pointing = create_dataframe(pointing_files)

    tracking_files = glob.glob(aux_folder + "/**/*DRIVE_CONTROL_TRACKING_POSITION.fits", recursive=True)
    print("Found {} tracking position files in path {}".format(len(tracking_files), aux_folder))
    df_tracking = create_dataframe(tracking_files)

    source_files = glob.glob(aux_folder + "/**/*DRIVE_CONTROL_SOURCE_POSITION.fits", recursive=True)
    print("Found {} source position files in path {}".format(len(source_files), aux_folder))
    df_source = create_dataframe(source_files)

    print("Writing {} entries to db. This might take a while".format(len(df_source)+len(df_tracking)))
    # print("Adding pointing data.")
    # df_pointing.to_sql('DRIVE_CONTROL_POINTING_POSITION', engine,  index=True, if_exists=behaviour , chunksize=chunksize)
    print("Adding tracking data")
    df_tracking.to_sql('DRIVE_CONTROL_TRACKING_POSITION', engine, index=True, if_exists=behaviour , chunksize=chunksize)
    print("Adding source data")
    df_source.to_sql('DRIVE_CONTROL_SOURCE_POSITION', engine, index=True,  if_exists=behaviour , chunksize=chunksize)

    # result = engine.execute('CREATE UNIQUE INDEX time_pointing ON DRIVE_CONTROL_POINTING_POSITION(time_in_seconds)')
    result = engine.execute('CREATE INDEX time_tracking ON DRIVE_CONTROL_TRACKING_POSITION(time_in_seconds)')
    result = engine.execute('CREATE INDEX time_source ON DRIVE_CONTROL_SOURCE_POSITION(time_in_seconds)')

    statinfo = os.stat(sqlite_file)
    print("DB has {} MegaBytes".format(statinfo.st_size/1000000.0))

def create_dataframe(files):
    df_source = pd.DataFrame()
    for f in tqdm(files):
        #catch some common errors in the fits files
        try:
            hdulist = fits.open(f)
            table = hdulist[1].data

            df = pd.DataFrame()
            for key in table.names:
                try:
                    df[key] = table[key]
                except ValuerError:
                    print('Swapped byteorder for key: ', key)
                    df[key] = table[key].byteswap().newbyteorder()
            df_source = df_source.append(df)

        except IndexError as e:
            print("File {} seems corrupted. Hdulist : {}".format(f, hdulist))
            print("Skipping to next file.")
            continue
        except OSError as e:
            print("File {} seems corrupted. Could not open file. Header corrupt?.  ".format(f))
            print("Skipping to next file.")
            continue
        except Exception as e:
            print("Errors occured while reading the fits file. ")
            print("Skipping to next file.")
            continue

    df_source['Time'] *=  86400
    df_source  = df_source.set_index(pd.to_datetime(df_source['Time'], unit='s'))
    df_source.rename(columns={'Time':'time_in_seconds'}, inplace=True)

    return df_source


if __name__ == "__main__":
    main()
