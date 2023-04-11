from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, targetSchema: str=None, sourceSystem: str=None, sourceDatabase: str=None, **kwargs):
        self.spark = None
        self.update(targetSchema, sourceSystem, sourceDatabase)

    def update(self, targetSchema: str="dev_md_l1", sourceSystem: str="mei", sourceDatabase: str="mei", **kwargs):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        pass
