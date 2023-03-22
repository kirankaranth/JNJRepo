from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, targetSchema: str=None, sourceSystem: str=None):
        self.spark = None
        self.update(targetSchema, sourceSystem)

    def update(self, targetSchema: str="l1_md_prophecy", sourceSystem: str="atl"):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.targetSchema = targetSchema
        self.sourceSystem = sourceSystem
        pass
