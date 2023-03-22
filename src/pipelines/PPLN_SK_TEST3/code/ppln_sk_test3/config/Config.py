from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, sourceSystem: str=None, targetSchema: str=None):
        self.spark = None
        self.update(sourceSystem, targetSchema)

    def update(self, sourceSystem: str="bba", targetSchema: str="l1_md_prophecy"):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        pass
