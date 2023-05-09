from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            MANDT: str=None,
            sourceSystem: str=None,
            TMST: str=None,
            DAI_ETL_ID: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceDatabase, DBTABLE, DBTABLE1, MANDT, sourceSystem, TMST, DAI_ETL_ID)

    def update(
            self,
            sourceDatabase: str="bba",
            DBTABLE: str="MBEW",
            DBTABLE1: str="MARA",
            MANDT: str="100",
            sourceSystem: str="bba",
            TMST: str="CASE WHEN TIMESTAMP = '0' THEN to_timestamp(NULL) ELSE to_timestamp(TIMESTAMP,'yyyyMMddHHmmss') END",
            DAI_ETL_ID: str="0",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.MANDT = MANDT
        self.sourceSystem = sourceSystem
        self.TMST = TMST
        self.DAI_ETL_ID = DAI_ETL_ID
        pass
