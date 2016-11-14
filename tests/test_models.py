from playhouse.test_utils import test_database


def test_init():
    from peewee import SqliteDatabase
    from erna.automatic_processing import models
    import tempfile

    with tempfile.NamedTemporaryFile() as f:
        test_db = SqliteDatabase(
            f.name,
            fields={'night': 'INTEGER', 'longblob': 'BLOB'}
        )
        with test_database(test_db, models.MODELS):
            models.init_database(test_db)
