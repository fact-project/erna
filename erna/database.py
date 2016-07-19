from peewee import (
    Model, MySQLDatabase, CharField, IntegerField, BooleanField,
    ForeignKeyField, CompositeKey, SqliteDatabase, DateField, Field
)
from datetime import date
import os

__all__ = ['RawDataFile', 'DrsFile', 'FactToolsRun']


class NightField(Field):
    db_field = 'night'

    def db_value(self, value):
        return 10000 * value.year + 100 * value.month + value.day

    def python_value(self, value):
        return date(value // 10000, value // 100, value % 100)


database = SqliteDatabase('test.sqlite')  # specify database at runtime


rawdirs = {
    'isdc': '/fact/raw',
    'phido': '/fhgfs/groups/app/fact/raw'
}


def init_database(drop=False):
    for model in (RawDataFile, DrsFile, FactToolsRun):
        if model.table_exists():
            if drop is True:
                model.drop_table()
                model.create_table()
        else:
            model.create_table()


class File(Model):
    night = NightField()
    run_id = IntegerField()
    available_dortmund = BooleanField(default=False)
    available_isdc = BooleanField(default=False)

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


class RawDataFile(File):
    @property
    def basename(self):
        return '{:%Y%m%d}_{:03d}.fits.fz'.format(self.night, self.run_id)


class DrsFile(File):
    @property
    def basename(self):
        return '{:%Y%m%d}_{:03d}.drs.fits.gz'.format(self.night, self.run_id)


class FactToolsRun(Model):
    raw_data_id = ForeignKeyField(RawDataFile, related_name='fact_tools_runs')
    drs_file_id = ForeignKeyField(DrsFile, related_name='fact_tools_runs')
    fact_tools_version = CharField()
    result_file = CharField()
    successful = BooleanField()

    class Meta:
        database = database
