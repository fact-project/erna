from peewee import (
    Model, CharField, IntegerField, BooleanField,
    ForeignKeyField, FixedCharField, TextField, MySQLDatabase
)
import os
import logging

from .utils import parse_path
from .custom_fields import NightField, LongBlobField


__all__ = [
    'RawDataFile', 'DrsFile',
    'Jar', 'XML', 'Job',
    'ProcessingState',
    'database', 'init_database',
]


log = logging.getLogger(__name__)

rawdirs = {
    'isdc': '/fact/raw',
    'phido': '/fhgfs/groups/app/fact/raw'
}

PROCESSING_STATES = [
    'inserted',
    'queued',
    'running',
    'success',
    'failed',
    'walltime_exceeded',
]

database = MySQLDatabase(None, fields={
    'night': 'INTEGER',
    'longblob': 'LONGBLOB',
})


def init_database(database, drop=False):
    tables = [
        RawDataFile,
        DrsFile,
        Jar,
        XML,
        Job,
        ProcessingState,
    ]
    if drop is True:
        log.info('dropping tables')
        database.drop_tables(tables, safe=True, cascade=True)

    database.create_tables(tables, safe=True)

    for description in PROCESSING_STATES:
        ProcessingState.get_or_create(description=description)


class File(Model):
    night = NightField()
    run_id = IntegerField()
    available_dortmund = BooleanField(null=True)
    available_isdc = BooleanField(null=True)

    class Meta:
        database = database
        indexes = ((('night', 'run_id'), True), )  # unique index

    def get_path(self, location='isdc'):
        return os.path.join(
            rawdirs[location],
            str(self.night.year),
            '{:02d}'.format(self.night.month),
            '{:02d}'.format(self.night.day),
            self.basename
        )

    @classmethod
    def get_or_create_from_path(cls, path):
        ''' Selects either an existing database entry or returns a new instance '''
        night, run_id = parse_path(path)
        return cls.get_or_create(night=night, run_id=run_id)

    def __repr__(self):
        return self.basename


class RawDataFile(File):
    @property
    def basename(self):
        return '{:%Y%m%d}_{:03d}.fits.fz'.format(self.night, self.run_id)

    class Meta:
        database = database
        db_table = 'raw_data_files'


class DrsFile(File):
    @property
    def basename(self):
        return '{:%Y%m%d}_{:03d}.drs.fits.gz'.format(self.night, self.run_id)

    class Meta:
        database = database
        db_table = 'drs_files'


class Jar(Model):
    version = CharField()
    data = LongBlobField()

    class Meta:
        database = database
        db_table = 'jars'


class XML(Model):
    name = CharField()
    content = TextField()
    comment = TextField()
    jar = ForeignKeyField(Jar)

    class Meta:
        database = database
        db_table = 'xmls'


class ProcessingState(Model):
    description = CharField(unique=True)

    class Meta:
        database = database
        db_table = 'processing_states'


class Job(Model):
    raw_data_file = ForeignKeyField(RawDataFile, related_name='raw_data_file')
    drs_file = ForeignKeyField(DrsFile, related_name='drs_file')
    jar = ForeignKeyField(Jar, related_name='jar')
    result_file = CharField(null=True)
    status = ForeignKeyField(ProcessingState, related_name='status')
    priority = IntegerField(default=5)
    xml = ForeignKeyField(XML)
    md5hash = FixedCharField(32, null=True)

    class Meta:
        database = database
        db_table = 'jobs'


MODELS = [RawDataFile, DrsFile, Jar, XML, Job, ProcessingState]
