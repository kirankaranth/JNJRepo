from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: str=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, sourceDatabase, DAI_ETL_ID, ConfigDatabase)

    def update(
            self,
            sourceSystem: str="bbl",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="bbl",
            DAI_ETL_ID: str="0",
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = DAI_ETL_ID
        self.ConfigDatabase = ConfigDatabase
        pass
