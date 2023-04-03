from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourcetable: str=None,
            DAI_ETL_ID: int=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, sourceDatabase, sourcetable, DAI_ETL_ID, configDatabase)

    def update(
            self,
            sourceSystem: str="jet",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="jet",
            sourcetable: str="f4102",
            DAI_ETL_ID: int=0,
            configDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourcetable = sourcetable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.configDatabase = configDatabase
        pass
