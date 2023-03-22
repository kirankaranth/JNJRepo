from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, SRC_SYS_C: str=None, MANDT: str=None, COLUMNS: int=None, DBNAME: str=None, DAI_ETL_ID: int=None):
        self.spark = None
        self.update(SRC_SYS_C, MANDT, COLUMNS, DBNAME, DAI_ETL_ID)

    def update(self, SRC_SYS_C: str="bbl", MANDT: str="100", COLUMNS: int=30, DBNAME: str="bbl", DAI_ETL_ID: int=0):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_C = SRC_SYS_C
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
