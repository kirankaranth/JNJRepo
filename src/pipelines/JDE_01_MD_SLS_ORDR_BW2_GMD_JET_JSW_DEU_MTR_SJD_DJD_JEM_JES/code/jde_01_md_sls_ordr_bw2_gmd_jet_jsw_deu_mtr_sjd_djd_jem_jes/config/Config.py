from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, targetSchema, MANDT, DAI_ETL_ID, sourceTable)

    def update(
            self,
            sourceSystem: str="bw2",
            sourceDatabase: str="bw2",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceTable: str="F4201",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        pass
