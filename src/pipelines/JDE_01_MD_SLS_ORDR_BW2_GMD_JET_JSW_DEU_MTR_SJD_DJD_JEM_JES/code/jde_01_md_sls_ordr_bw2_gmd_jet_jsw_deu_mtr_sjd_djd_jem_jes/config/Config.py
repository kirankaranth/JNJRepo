from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, targetSchema, DAI_ETL_ID, sourceTable, ConfigDatabase)

    def update(
            self,
            sourceSystem: str="bw2",
            sourceDatabase: str="bw2",
            targetSchema: str="dev_md_l1",
            DAI_ETL_ID: int=0,
            sourceTable: str="F4201",
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.ConfigDatabase = ConfigDatabase
        pass
