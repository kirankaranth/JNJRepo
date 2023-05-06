from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            MANDT: str=None,
            SRC_SYS_CD: str=None,
            TMST: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceDatabase, DBTABLE, DBTABLE1, MANDT, SRC_SYS_CD, TMST)

    def update(
            self,
            sourceDatabase: str="bba",
            DBTABLE: str="MBEW",
            DBTABLE1: str="MARA",
            MANDT: str="100",
            SRC_SYS_CD: str="bba",
            TMST: str="CASE WHEN TIMESTAMP = '0' THEN CAST(NULL AS TIMESTAMP) ELSE timestamp(TIMESTAMP'yyyyMMddHHmmss') END",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.MANDT = MANDT
        self.SRC_SYS_CD = SRC_SYS_CD
        self.TMST = TMST
        pass
