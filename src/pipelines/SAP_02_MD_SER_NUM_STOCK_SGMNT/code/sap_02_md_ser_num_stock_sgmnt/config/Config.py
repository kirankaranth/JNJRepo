from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None, 
            targetSchema: str=None, 
            MANDT: str=None, 
            DBNAME: str=None, 
            DAI_ETL_ID: int=None
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, DBNAME, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="hcs", 
            targetSchema: str="l1_md_prophecy", 
            MANDT: str="100", 
            DBNAME: str="hcs", 
            DAI_ETL_ID: int=0
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
