from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None, 
            MANDT: str=None, 
            DBNAME: str=None, 
            DAI_ETL_ID: int=None, 
            targetSchema: str=None
    ):
        self.spark = None
        self.update(sourceSystem, MANDT, DBNAME, DAI_ETL_ID, targetSchema)

    def update(
            self,
            sourceSystem: str="bbn", 
            MANDT: str="100", 
            DBNAME: str="bbn", 
            DAI_ETL_ID: int=0, 
            targetSchema: str="l1_md_prophecy"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        pass
