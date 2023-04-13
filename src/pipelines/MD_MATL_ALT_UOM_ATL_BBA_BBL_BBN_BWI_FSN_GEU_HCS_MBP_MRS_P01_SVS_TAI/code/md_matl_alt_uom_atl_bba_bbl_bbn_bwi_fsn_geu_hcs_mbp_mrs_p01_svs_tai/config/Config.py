from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            DBNAME: str=None,
            DBTABLE: str=None,
            COLUMNS: int=None,
            DAI_ETL_ID: int=None,
            DBTABLE1: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, DBNAME, DBTABLE, COLUMNS, DAI_ETL_ID, DBTABLE1)

    def update(
            self,
            sourceSystem: str="bba",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            DBNAME: str="bba",
            DBTABLE: str="marm",
            COLUMNS: int=28,
            DAI_ETL_ID: int=0,
            DBTABLE1: str="mara",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.DBNAME = DBNAME
        self.DBTABLE = DBTABLE
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.DBTABLE1 = DBTABLE1
        pass
