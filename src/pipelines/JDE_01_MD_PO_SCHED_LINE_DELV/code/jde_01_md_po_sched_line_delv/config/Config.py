from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None,
            DBNAME: str=None,
            TABLENAME: str=None,
            DAI_ETL_ID: int=None,
            COLUMNS: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(SRC_SYS_CD, DBNAME, TABLENAME, DAI_ETL_ID, COLUMNS)

    def update(
            self,
            SRC_SYS_CD: str="jes",
            DBNAME: str="jes",
            TABLENAME: str="f4311",
            DAI_ETL_ID: int=0,
            COLUMNS: int=21,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.DBNAME = DBNAME
        self.TABLENAME = TABLENAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.COLUMNS = self.get_int_value(COLUMNS)
        pass
