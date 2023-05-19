from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            PRC_BND_CAT: str=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceDatabase, sourceSystem, MANDT, DAI_ETL_ID, targetSchema, PRC_BND_CAT, ConfigDatabase)

    def update(
            self,
            sourceDatabase: str="bbl",
            sourceSystem: str="bbl",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            PRC_BND_CAT: str="trim(PLGTP)",
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.PRC_BND_CAT = PRC_BND_CAT
        self.ConfigDatabase = ConfigDatabase
        pass
