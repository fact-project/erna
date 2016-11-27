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
    'FACTToolsVersion', 'FACTToolsXML', 'FACTToolsRun',
    'ProcessingState',
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
        FACTToolsVersion,
        FACTToolsXML,
        FACTToolsRun,
        ProcessingState,
    ]
    if drop is True:
        log.info('dropping tables')
        database.drop_tables(tables, safe=True, cascade=True)

    database.create_tables(tables, safe=True)

    for description in PROCESSING_STATES:
        try:
            (
                ProcessingState
                .select()
                .where(ProcessingState.description == description)
                .get()
            )
        except ProcessingState.DoesNotExist:
            ProcessingState.create(description=description)


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
    def select_from_path(cls, path):
        ''' Selects either an existing database entry or returns a new instance '''
        log.debug('In from_path')
        night, run_id = parse_path(path)
        return cls.select_night_runid(night, run_id)

    @classmethod
    def select_night_runid(cls, night, run_id):
        ''' Selects either an existing database entry or returns a new instance '''
        try:
            run = cls.select().where((cls.night == night) & (cls.run_id == run_id)).get()
            log.debug('returnig existing instance')
            return run
        except cls.DoesNotExist:
            log.debug('returnig new instance')
            return cls(night=night, run_id=run_id)

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


class FACTToolsVersion(Model):
    version = CharField(primary_key=True)
    jar_file = LongBlobField()

    class Meta:
        database = database
        db_table = 'fact_tools_versions'


class FACTToolsXML(Model):
    name = CharField()
    content = TextField()
    comment = TextField()
    fact_tools_version = ForeignKeyField(FACTToolsVersion)

    class Meta:
        database = database
        db_table = 'fact_tools_xmls'


class ProcessingState(Model):
    description = CharField(unique=True)

    class Meta:
        database = database
        db_table = 'processing_states'


class FACTToolsRun(Model):
    raw_data_id = ForeignKeyField(RawDataFile, related_name='fact_tools_runs')
    drs_file_id = ForeignKeyField(DrsFile, related_name='fact_tools_runs')
    fact_tools_version = ForeignKeyField(
        FACTToolsVersion, related_name='fact_tools_version'
    )
    result_file = CharField()
    status = ForeignKeyField(ProcessingState, related_name='status')
    priority = IntegerField(default=5)
    xml = ForeignKeyField(FACTToolsXML)
    md5hash = FixedCharField(32, null=True)

    class Meta:
        database = database
        db_table = 'fact_tools_runs'


MODELS = [
    RawDataFile, DrsFile, FACTToolsVersion, FACTToolsXML, FACTToolsRun, ProcessingState,
]
