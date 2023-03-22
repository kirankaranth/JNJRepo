from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, sourceSystem: str=None, sourceDatabase: str=None, targetSchema: str=None):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, targetSchema)

    def update(self, sourceSystem: str="atl", sourceDatabase: str="atlas", targetSchema: str="l1_md_prophecy"):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        pass
