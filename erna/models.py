from peewee import MySQLDatabase, Model, IntegerField, FloatField,
BooleanField, DateTimeField, TextField, CharField

db = MySQLDatabase("factdata")


class BaseModel(Model):

    class Meta:
        database = db


class FactToolsProcessing(BaseModel):
    night = IntegerField()
    run_id = IntegerField()
    used_drs_file = TextField(null=True)
    fact_tools_jar = TextField(null=True)
    output_path = TextField(null=True)
    status = CharField(null=True)


class FactRawData(BaseModel):
    night = IntegerField()
    run_id = IntegerField()
    avail_tudo = BooleanField(null=True)
    avail_ISDC = BooleanField(null=True)
