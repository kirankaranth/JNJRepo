from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(SRC_SYS_CD, MANDT, DAI_ETL_ID, sourceTable, targetSchema, sourceDatabase)

    def update(
            self,
            SRC_SYS_CD: str="bbl",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceTable: str="mvke",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="bbl",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        pass
