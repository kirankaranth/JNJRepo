from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            sourceTable: str=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, MANDT, sourceDatabase, DAI_ETL_ID, targetSchema, sourceTable, configDatabase)

    def update(
            self,
            sourceSystem: str="bbl",
            MANDT: str="100",
            sourceDatabase: str="bbl",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            sourceTable: str="eket",
            configDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.sourceTable = sourceTable
        self.configDatabase = configDatabase
        pass
