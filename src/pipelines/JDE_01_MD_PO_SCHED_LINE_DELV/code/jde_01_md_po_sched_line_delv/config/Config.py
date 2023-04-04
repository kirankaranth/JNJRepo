from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, sourceTable, DAI_ETL_ID, targetSchema, configDatabase)

    def update(
            self,
            sourceSystem: str="jes",
            sourceDatabase: str="jes",
            sourceTable: str="f4311",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_l1_md",
            configDatabase: str=None,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.configDatabase = configDatabase
        pass
