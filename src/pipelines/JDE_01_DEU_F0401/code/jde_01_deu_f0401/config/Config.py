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
            sourceTable: str=None,
            sourceTable1: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            MANDT, 
            sourceDatabase, 
            DAI_ETL_ID, 
            ConfigDatabase, 
            sourceTable, 
            sourceTable1
        )

    def update(
            self,
            sourceSystem: str="due",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="due",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=None,
            sourceTable: str="F0401",
            sourceTable1: str="F0101",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.sourceTable = sourceTable
        self.sourceTable1 = sourceTable1
        pass
