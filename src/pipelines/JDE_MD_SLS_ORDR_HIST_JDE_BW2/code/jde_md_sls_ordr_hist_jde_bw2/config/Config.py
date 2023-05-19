from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            DAI_ETL_ID: str=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(targetSchema, sourceDatabase, sourceSystem, DAI_ETL_ID, configDatabase)

    def update(
            self,
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="bw2",
            sourceSystem: str="bw2",
            DAI_ETL_ID: str="0",
            configDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.DAI_ETL_ID = DAI_ETL_ID
        self.configDatabase = configDatabase
        pass
