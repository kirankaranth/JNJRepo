from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            DBTable1: str=None,
            DBTable2: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, sourceDatabase, DAI_ETL_ID, ConfigDatabase, DBTable1, DBTable2)

    def update(
            self,
            sourceSystem: str="mbp",
            targetSchema: str="dev_md_l1",
            MANDT: str="600",
            sourceDatabase: str="mbp",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            DBTable1: str="mcha",
            DBTable2: str="mch1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.DBTable1 = DBTable1
        self.DBTable2 = DBTable2
        pass
