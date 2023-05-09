from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            MANDT: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            TMST: str=None,
            DAI_ETL_ID: str=None,
            configDatabase: str=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceDatabase, 
            sourceSystem, 
            MANDT, 
            DBTABLE, 
            DBTABLE1, 
            TMST, 
            DAI_ETL_ID, 
            configDatabase, 
            targetSchema
        )

    def update(
            self,
            sourceDatabase: str="bba",
            sourceSystem: str="bba",
            MANDT: str="100",
            DBTABLE: str="MBEW",
            DBTABLE1: str="MARA",
            TMST: str="CASE WHEN TIMESTAMP = '0' THEN to_timestamp(NULL) ELSE to_timestamp(TIMESTAMP,'yyyyMMddHHmmss') END",
            DAI_ETL_ID: str="0",
            configDatabase: str=" ",
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.TMST = TMST
        self.DAI_ETL_ID = DAI_ETL_ID
        self.configDatabase = configDatabase
        self.targetSchema = targetSchema
        pass
