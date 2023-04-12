from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            sourceTable1: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, sourceTable, sourceTable1, DAI_ETL_ID, targetSchema, ConfigDatabase)

    def update(
            self,
            sourceSystem: str="bw2",
            sourceDatabase: str="bw2",
            sourceTable: str="F43121",
            sourceTable1: str="F4211",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.sourceTable1 = sourceTable1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.ConfigDatabase = ConfigDatabase
        pass
