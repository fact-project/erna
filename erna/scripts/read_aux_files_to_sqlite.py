from astropy.io import fits
from IPython import embed
import pandas as pd
from sqlalchemy import create_engine
import glob
import os
import click
from tqdm import tqdm


@click.command()
@click.argument('aux_folder', nargs=-1, type=click.Path(file_okay=False, dir_okay=True, exists=True))
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
    drive_files = []
    for aux in aux_folder:
        drive_files += glob.glob(aux + "/**/*DRIVE_CONTROL_[TS]*_POSITION.fits", recursive=True)
    if not drive_files:
        print("No files found. Wrong path?")
    print("Found {} position files in path {}".format(len(drive_files), aux_folder))
    df_source = pd.DataFrame()
    df_tracking = pd.DataFrame()

    keys_source=['Time', 'Name', 'Ra_src', 'Dec_src', 'Angle', 'Offset', 'QoS']
    keys_tracking=['Time', 'Ra', 'Dec', 'Az', 'Zd', 'Ha', 'dZd', 'dAz', 'dev', 'QoS']
    for f in tqdm(drive_files):
        if "SOURCE" in f:
            df_source = append_to_dataframe(df_source, f, keys_source)
        if "TRACKING" in f:
            df_tracking = append_to_dataframe(df_tracking, f, keys_tracking)


    df_source = create_time_index(df_source)
    df_tracking = create_time_index(df_tracking)


    print("Writing {} entries to db. This might take a while".format(len(df_source)+len(df_tracking)))
    # print("Adding pointing data.")
    # df_pointing.to_sql('DRIVE_CONTROL_POINTING_POSITION', engine,  index=True, if_exists=behaviour , chunksize=chunksize)
    print("Adding tracking data")
    df_tracking.to_sql('DRIVE_CONTROL_TRACKING_POSITION', engine, index=True, if_exists=behaviour , chunksize=chunksize)
    print("Adding source data")
    df_source.to_sql('DRIVE_CONTROL_SOURCE_POSITION', engine, index=True,  if_exists=behaviour , chunksize=chunksize)

    print("Creating index")
    # result = engine.execute('CREATE UNIQUE INDEX time_pointing ON DRIVE_CONTROL_POINTING_POSITION(time_in_seconds)')
    engine.execute('CREATE INDEX time_tracking ON DRIVE_CONTROL_TRACKING_POSITION(time_in_seconds)')
    engine.execute('CREATE INDEX time_source ON DRIVE_CONTROL_SOURCE_POSITION(time_in_seconds)')

    statinfo = os.stat(sqlite_file)
    print("DB has approximatly {} MegaBytes".format(statinfo.st_size/1000000.0))

def create_time_index(df):
    df['Time'] *=  86400
    df  = df.set_index(pd.to_datetime(df['Time'], unit='s'))
    df.rename(columns={'Time':'time_in_seconds'}, inplace=True)
    return df

def append_to_dataframe(df_aux, drive_file, keys):
    try:
        hdulist = fits.open(drive_file)
        table = hdulist[1].data
        df = pd.DataFrame()
        for key in keys:
            try:
                df[key] = table[key]
            except ValueError:
                print('Swapped byteorder for key: ', key)
                df[key] = table[key].byteswap().newbyteorder()
        df_aux = df_aux.append(df)
        return df_aux

    except IndexError:
        print("File {} seems corrupted. Hdulist : {}".format(drive_file, hdulist))
        print("Skipping to next file.")
    except OSError:
        print("File {} seems corrupted. Could not open file. Header corrupt?.  ".format(drive_file))
        print("Skipping to next file.")
    except Exception as horst:
        print(horst)
        print("Errors occured while reading the fits file. ")
        print("Skipping to next file.")
    finally:
        return df_aux


if __name__ == "__main__":
    main()
