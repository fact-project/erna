import yaml
from erna.database import database

database.init(**yaml.safe_load(open("config.yaml")))
