from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            DAI_ETL_ID: int=None,
            MANDT: str=None,
            configDatabase: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, DAI_ETL_ID, MANDT, configDatabase, sourceDatabase, targetSchema)

    def update(
            self,
            sourceSystem: str="hm2",
            DAI_ETL_ID: int=0,
            MANDT: str="100",
            configDatabase: str=" ",
            sourceDatabase: str="hm2",
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.MANDT = MANDT
        self.configDatabase = configDatabase
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        pass
