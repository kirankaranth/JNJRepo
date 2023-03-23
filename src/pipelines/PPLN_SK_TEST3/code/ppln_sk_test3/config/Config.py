from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, sourceSystem: str=None, targetSchema: str=None, SAVA_FIELD_1: str=None, SAVA_FIELD_2: str=None):
        self.spark = None
        self.update(sourceSystem, targetSchema, SAVA_FIELD_1, SAVA_FIELD_2)

    def update(
            self,
            sourceSystem: str="bba", 
            targetSchema: str="l1_md_prophecy", 
            SAVA_FIELD_1: str="no", 
            SAVA_FIELD_2: str="no"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.SAVA_FIELD_1 = SAVA_FIELD_1
        self.SAVA_FIELD_2 = SAVA_FIELD_2
        pass
