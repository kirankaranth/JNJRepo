from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None, 
            targetSchema: str=None, 
            MANDT: str=None, 
            DAI_ETL_ID: str=None, 
            DBNAME: str=None
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, DAI_ETL_ID, DBNAME)

    def update(
            self,
            sourceSystem: str="mrs", 
            targetSchema: str="l1_md_prophecy", 
            MANDT: str="100", 
            DAI_ETL_ID: str="0", 
            DBNAME: str="mrs"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.DAI_ETL_ID = DAI_ETL_ID
        self.DBNAME = DBNAME
        pass
