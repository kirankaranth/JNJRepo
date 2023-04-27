from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceTable: str=None,
            sourceDatabase: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceSystem: str=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceTable, sourceDatabase, MANDT, DAI_ETL_ID, sourceSystem, targetSchema)

    def update(
            self,
            sourceTable: str="RSEG",
            sourceDatabase: str="hm2",
            MANDT: str="500",
            DAI_ETL_ID: int=0,
            sourceSystem: str="HM2",
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceTable = sourceTable
        self.sourceDatabase = sourceDatabase
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        pass
