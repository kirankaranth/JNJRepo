from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            DAI_ETL_ID: int=None,
            DIVISOR: int=None,
            configDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(targetSchema, sourceDatabase, sourceSystem, DBTABLE, DBTABLE1, DAI_ETL_ID, DIVISOR, configDatabase)

    def update(
            self,
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="gmd",
            sourceSystem: str="gmd",
            DBTABLE: str="f4101  ",
            DBTABLE1: str="f41021  ",
            DAI_ETL_ID: int=0,
            DIVISOR: int=1,
            configDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.DIVISOR = self.get_int_value(DIVISOR)
        self.configDatabase = configDatabase
        pass
