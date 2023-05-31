from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            F4801: str=None,
            configDatabase: str=None,
            targetSchema: str=None,
            DAI_ETL_ID: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, sourceDatabase, F4801, configDatabase, targetSchema, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="gmd",
            sourceDatabase: str="gmd",
            F4801: str="F4801",
            configDatabase: str=" ",
            targetSchema: str="dev_md_l1",
            DAI_ETL_ID: str="0",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.F4801 = F4801
        self.configDatabase = configDatabase
        self.targetSchema = targetSchema
        self.DAI_ETL_ID = DAI_ETL_ID
        pass
