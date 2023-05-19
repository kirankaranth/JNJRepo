from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            targetSchema: str=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceDatabase, sourceSystem, DAI_ETL_ID, sourceTable, targetSchema, ConfigDatabase)

    def update(
            self,
            sourceDatabase: str="deu",
            sourceSystem: str="deu",
            DAI_ETL_ID: int=0,
            sourceTable: str="f4101",
            targetSchema: str="dev_md_l1",
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.targetSchema = targetSchema
        self.ConfigDatabase = ConfigDatabase
        pass
