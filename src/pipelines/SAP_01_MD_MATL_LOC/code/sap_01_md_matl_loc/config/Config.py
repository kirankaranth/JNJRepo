from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None, 
            targetSchema: str=None, 
            MANDT: str=None, 
            COLUMNS: int=None, 
            DBNAME: str=None, 
            DAI_ETL_ID: int=None
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, COLUMNS, DBNAME, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="hm2", 
            targetSchema: str="l1_md_prophecy", 
            MANDT: str="100", 
            COLUMNS: int=134, 
            DBNAME: str="pqa_hm2", 
            DAI_ETL_ID: int=0
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
