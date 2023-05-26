from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            MANDT: str=None,
            configDatabase: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceDatabase, sourceSystem, MANDT, configDatabase, DAI_ETL_ID, targetSchema)

    def update(
            self,
            sourceDatabase: str="bbl",
            sourceSystem: str="bbl",
            MANDT: str="100",
            configDatabase: str=" ",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.configDatabase = configDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        pass
