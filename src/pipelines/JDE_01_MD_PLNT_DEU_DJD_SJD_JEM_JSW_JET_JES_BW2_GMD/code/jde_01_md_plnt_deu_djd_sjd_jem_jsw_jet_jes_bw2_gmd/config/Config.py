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
            sourceTable1: str=None,
            STf0101: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            sourceDatabase, 
            DAI_ETL_ID, 
            ConfigDatabase, 
            sourceTable, 
            sourceTable1, 
            STf0101
        )

    def update(
            self,
            sourceSystem: str="deu",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="deu",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            sourceTable: str="f0006",
            sourceTable1: str="f0116",
            STf0101: str="f0101",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.sourceTable = sourceTable
        self.sourceTable1 = sourceTable1
        self.STf0101 = STf0101
        pass
