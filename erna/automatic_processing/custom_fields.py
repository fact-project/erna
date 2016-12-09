from peewee import Field, BlobField
import logging
from datetime import date


log = logging.getLogger(__name__)


def night_int_to_date(night):
    return date(night // 10000, (night % 10000) // 100, night % 100)


class NightField(Field):
    db_field = 'night'

    def db_value(self, value):
        return 10000 * value.year + 100 * value.month + value.day

    def python_value(self, value):
        return night_int_to_date(value)


class LongBlobField(BlobField):
    db_field = 'longblob'
