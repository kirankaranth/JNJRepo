from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: str=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, targetSchema, sourceTable, DAI_ETL_ID, configDatabase)

    def update(
            self,
            sourceSystem: str="jem",
            sourceDatabase: str="jem",
            targetSchema: str="dev_md_l1",
            sourceTable: str="f43199",
            DAI_ETL_ID: str="0",
            configDatabase: str=None,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = DAI_ETL_ID
        self.configDatabase = configDatabase
        pass
