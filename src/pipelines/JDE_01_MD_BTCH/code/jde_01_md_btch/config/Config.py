from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            sourceTable: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, sourceDatabase, DAI_ETL_ID, ConfigDatabase, sourceTable)

    def update(
            self,
            sourceSystem: str="jem",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="jem",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            sourceTable: str="f4108",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.sourceTable = sourceTable
        pass
