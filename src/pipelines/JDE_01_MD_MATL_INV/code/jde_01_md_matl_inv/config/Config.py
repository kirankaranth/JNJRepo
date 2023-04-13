from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            DBNAME: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            COLUMNS: int=None,
            DAI_ETL_ID: int=None,
            DIVISOR: int=None,
            ConfigDatabase: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, DBNAME, DBTABLE, DBTABLE1, COLUMNS, DAI_ETL_ID, DIVISOR, ConfigDatabase)

    def update(
            self,
            sourceSystem: str="gmd",
            targetSchema: str="dev_md_l1",
            DBNAME: str="gmd",
            DBTABLE: str="f4101  ",
            DBTABLE1: str="f41021  ",
            COLUMNS: int=24,
            DAI_ETL_ID: int=0,
            DIVISOR: int=1,
            ConfigDatabase: str=" ",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.DBNAME = DBNAME
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.DIVISOR = self.get_int_value(DIVISOR)
        self.ConfigDatabase = ConfigDatabase
        pass
