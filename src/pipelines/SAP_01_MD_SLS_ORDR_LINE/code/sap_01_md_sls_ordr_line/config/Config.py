from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            MANDT: str=None, 
            COLUMNS: str=None, 
            DBNAME: str=None, 
            DAI_ETL_ID: int=None
    ):
        self.spark = None
        self.update(SRC_SYS_CD, MANDT, COLUMNS, DBNAME, DAI_ETL_ID)

    def update(self, SRC_SYS_CD: str="bbl", MANDT: str="100", COLUMNS: str="100", DBNAME: str="bbl", DAI_ETL_ID: int=0):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.MANDT = MANDT
        self.COLUMNS = COLUMNS
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
