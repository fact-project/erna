# Easy RuN Access (ERNA)

A collection of tools to handle FACT data and to execute jobs on a SGE/TORQUE cluster.

![http://www.itbusiness.ca/wp-content/uploads/2012/10/Old-women-on-laptop.jpg](http://www.itbusiness.ca/wp-content/uploads/2012/10/Old-women-on-laptop.jpg)

The erna module allows you to find datafiles and the optimal (closest) drs-files for a chosen period of time.
In the main you can choose the parameters for your search (at the moment: the beginning and the end of the requested period of time (*earliest_run* and *latest_run*), the source (*source_name*) and *timedelta_in_minutes*).
The parameter *timedelta_in_minutes* determines the maximum allowed time lag between the timestamp of the data-file and the timestamp of the appropriate drs-file. The default value is 30 minutes. Which works fine in my experience.
Dates are given in the usual FACT convention: YYYYMMDD.

## Requirements
  - FACT-Tools
  - Java 1.8 (module add java on lido3)
  - Python 3.5+


## execute_data_processing.py

The `execute_data_processing.py` executable calls the *erna* module and creates jobs for submission to a grid. This needs to be run from a server which can submit jobs to the queueing system.
You need to provide the path to the fact-tools.jar file and the xml you want to use.
An example xml can be found in the repository. It shows how to read fact data files and how to output the result. The resulting json files are automatically collected and merged into one big outputfile. The data format of this outputfile depends on the name you choose. (e.g. big_output_file.h5, big_output_file.json, big_output_file.csv,...)

You cann call execute like this:

         python erna/execute_data_processing.py 20140101 20140130 /fhgfs/groups/app/fact/raw/ fact-tools-0.11.2 example.xml big_output_file.h5 --engine=PBS --vmem=10000 --num_jobs=60 --queue=one_day --source=Crab

When calling the `execute_list.py` script you get


## fetch_runs.py

The script creates a json-file (*earliest_run_latest_run_source_name.json*), which contains the found data and drs-files
The created file can be used in conjunction with `execute_list.py` to start jobs with the files contained in the .json file.
This is useful if you don't have internet access on the machine where you submit your jobs from. *coughISDCcough*

# Automatic processing with erna at isdc

The erna job submitter should be running as `fact_tools` user on
isdc-in04.

To run it, start the screen session with the screenrc coming in this repo:
```
$ screen -c screenrc_erna
```

It sets up the necessary port forwarding and proxy to speak to the non-isdc world and starts the erna submitter processing.

## Submit runs:

We provide a script `erna_submit_runlist` to submit a csv runlist into the
automatic processing.


Alternatively, you can use the erna_console and enter: 

```python
files = (
    RawDataFile
    .select()
    .where(RawDataFile.night >= date(2013, 1, 1))
    .where(RawDataFile.night <= date(2013, 12, 31))
)
print("files.count():", files.count())

# We only select Jar.id and Jar.version to not download the 20 MB binary blob
jar = Jar.select(Jar.id, Jar.version).where(Jar.version == '0.17.2').get()
xml = XML.get(name='std_analysis', jar=jar)
queue = Queue.get(name='fact_short')

insert_new_jobs(files, xml=xml, jar=jar, queue=queue)
```
