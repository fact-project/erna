# Easy RuN Access (ERNA)
A collection of tools to handle FACT data and to execute jobs on a SGE/TORQUE cluster.

These scripts require a few dependencies. More documentation to follow.

## erna.py
The erna.py script allows you to find datafiles and the optimal (closest) drs-files for a chosen period of time.
In the main you can choose the parameters for your search (at the moment: the beginning and the end of the requested period of time (*earliest_run* and *latest_run*), the source (*source_name*) and *timedelta_in_minutes*).
The parameter *timedelta_in_minutes* determines the maximum allowed time lag between the timestamp of the data-file and the timestamp of the appropriate drs-file.
The script creates a json-file (*earliest_run_latest_run_source_name.json*), which contains the found data and drs-files


## execute.py

The execute.py script calls the *erna* module and creates jobs for submission to a grid. This needs to be run from a server which can submit jobs to the queueing system.
