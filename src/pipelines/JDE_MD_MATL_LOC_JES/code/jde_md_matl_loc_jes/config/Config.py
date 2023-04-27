from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, sourceDatabase, sourceTable, DAI_ETL_ID, configDatabase)

    def update(
            self,
            sourceSystem: str="jet",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="jet",
            sourceTable: str="f4102_adt",
            DAI_ETL_ID: int=0,
            configDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.configDatabase = configDatabase
        pass
