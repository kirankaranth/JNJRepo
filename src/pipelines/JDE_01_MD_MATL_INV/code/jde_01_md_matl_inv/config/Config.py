from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            DBNAME: str=None, 
            DBTABLE: str=None, 
            DBTABLE1: str=None, 
            COLUMNS: int=None, 
            DAI_ETL_ID: str=None, 
            DIVISOR: int=None
    ):
        self.spark = None
        self.update(SRC_SYS_CD, DBNAME, DBTABLE, DBTABLE1, COLUMNS, DAI_ETL_ID, DIVISOR)

    def update(
            self,
            SRC_SYS_CD: str="gmd", 
            DBNAME: str="gmd", 
            DBTABLE: str="f4101  ", 
            DBTABLE1: str="f41021  ", 
            COLUMNS: int=24, 
            DAI_ETL_ID: str="1", 
            DIVISOR: int=1
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.DBNAME = DBNAME
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DAI_ETL_ID = DAI_ETL_ID
        self.DIVISOR = self.get_int_value(DIVISOR)
        pass
