from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            MANDT: str=None,
            targetSchema: str=None,
            configDatabase: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceDatabase, sourceSystem, MANDT, targetSchema, configDatabase, DAI_ETL_ID)

    def update(
            self,
            sourceDatabase: str="atl",
            sourceSystem: str="atl",
            MANDT: str="100",
            targetSchema: str="dev_md_l1",
            configDatabase: str=" ",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.targetSchema = targetSchema
        self.configDatabase = configDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
