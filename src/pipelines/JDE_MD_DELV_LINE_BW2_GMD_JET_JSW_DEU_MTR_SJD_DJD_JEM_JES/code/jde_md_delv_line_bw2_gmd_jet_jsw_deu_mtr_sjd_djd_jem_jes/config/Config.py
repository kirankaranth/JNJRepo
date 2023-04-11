from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            ConfigDatabase: str=None,
            DBTABLE1: str=None,
            DBTABLE2: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            sourceDatabase, 
            sourceTable, 
            DAI_ETL_ID, 
            targetSchema, 
            ConfigDatabase, 
            DBTABLE1, 
            DBTABLE2
        )

    def update(
            self,
            sourceSystem: str="bw2",
            sourceDatabase: str="bw2",
            sourceTable: str="F43121, F4211",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            ConfigDatabase: str=" ",
            DBTABLE1: str="F43121",
            DBTABLE2: str="F4211",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.ConfigDatabase = ConfigDatabase
        self.DBTABLE1 = DBTABLE1
        self.DBTABLE2 = DBTABLE2
        pass
