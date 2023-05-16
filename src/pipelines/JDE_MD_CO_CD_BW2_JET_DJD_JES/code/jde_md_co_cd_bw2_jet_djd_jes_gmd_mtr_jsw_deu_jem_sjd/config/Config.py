from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceTable: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, sourceTable, sourceDatabase, DAI_ETL_ID, ConfigDatabase)

    def update(
            self,
            sourceSystem: str="bw2",
            targetSchema: str="dev_md_l1",
            sourceTable: str="f0101_adt",
            sourceDatabase: str="bw2",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceTable = sourceTable
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        pass
