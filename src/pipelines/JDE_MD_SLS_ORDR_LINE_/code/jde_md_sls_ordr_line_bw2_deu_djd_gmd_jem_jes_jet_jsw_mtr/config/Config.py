from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            sourceTable1: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, sourceDatabase, sourceTable, sourceTable1, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="jet",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="jet",
            sourceTable: str="f42119",
            sourceTable1: str="f4211",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.sourceTable1 = sourceTable1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
