## Easy RuN Access (ERNA)

This repository contains a python script, which allows you to find datafiles and the optimal (closest) drs-files for a chosen period of time.
In the main you can choose the parameters for your search (at the moment: the beginning and the end of the requested period of time (*earliest_run* and *latest_run*), the source (*source_name*) and *timedelta_in_minutes*).
The parameter *timedelta_in_minutes* determines the time lag between the timestamp of the data-file and the timestamp of the appropriate drs-file. 
The script creates a json-file (*'earliest_run''_'latest_run''_'source_name'.json*), which contains the found data and drs-files:

{"20150206_024":{"drs_path":"\/fact\/raw\/2015\/02\/06\/20150206_007.drs.fits.gz","data_path":"\/fact\/raw\/2015\/02\/06\/20150206_024.fits.fz"},...}

Currently the output is limited to one drs-data-pair per night. 