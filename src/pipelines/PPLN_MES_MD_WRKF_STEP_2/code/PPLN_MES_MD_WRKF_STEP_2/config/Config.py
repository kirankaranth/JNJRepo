from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, targetSchema: str=None, sourceSystem: str=None, sourceDatabase: str=None, **kwargs):
        self.spark = None
        self.update(targetSchema, sourceSystem, sourceDatabase)

    def update(self, targetSchema: str="l1_md_prophecy", sourceSystem: str="cmw", sourceDatabase: str="cmw", **kwargs):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        pass
