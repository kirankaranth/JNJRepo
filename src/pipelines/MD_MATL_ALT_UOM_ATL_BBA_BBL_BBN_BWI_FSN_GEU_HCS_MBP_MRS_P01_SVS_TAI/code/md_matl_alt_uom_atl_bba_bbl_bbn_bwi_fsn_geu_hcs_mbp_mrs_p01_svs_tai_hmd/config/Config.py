from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            configDatabase: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(targetSchema, sourceDatabase, sourceSystem, configDatabase, MANDT, DAI_ETL_ID)

    def update(
            self,
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="bba",
            sourceSystem: str="bba",
            configDatabase: str=" ",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.configDatabase = configDatabase
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
