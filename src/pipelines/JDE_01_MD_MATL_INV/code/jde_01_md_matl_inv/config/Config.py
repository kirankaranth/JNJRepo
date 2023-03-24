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
            DAI_ETL_ID: str=None,
            DIVISOR: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, DBNAME, DBTABLE, DBTABLE1, COLUMNS, DAI_ETL_ID, DIVISOR)

    def update(
            self,
            sourceSystem: str="gmd",
            targetSchema: str="l1_md_prophecy",
            DBNAME: str="gmd",
            DBTABLE: str="f4101  ",
            DBTABLE1: str="f41021  ",
            COLUMNS: int=24,
            DAI_ETL_ID: str="1",
            DIVISOR: int=1,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.DBNAME = DBNAME
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DAI_ETL_ID = DAI_ETL_ID
        self.DIVISOR = self.get_int_value(DIVISOR)
        pass
