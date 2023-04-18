from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabse: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, sourceDatabase, DAI_ETL_ID, ConfigDatabse)

    def update(
            self,
            sourceSystem: str="fsn",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="fsn",
            DAI_ETL_ID: int=0,
            ConfigDatabse: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabse = ConfigDatabse
        pass
