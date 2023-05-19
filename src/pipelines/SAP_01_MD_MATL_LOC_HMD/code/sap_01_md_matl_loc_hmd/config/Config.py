from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, sourceDatabase, sourceTable, DAI_ETL_ID, ConfigDatabase)

    def update(
            self,
            sourceSystem: str="hm2",
            targetSchema: str="dev_md_l1",
            MANDT: str="500",
            sourceDatabase: str="hm2",
            sourceTable: str="NSDM_V_MARC",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        pass
