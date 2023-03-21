from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            DBNAME: str=None, 
            TABLENAME: str=None, 
            DAI_ETL_ID: str=None, 
            COLUMNS: str=None
    ):
        self.spark = None
        self.update(SRC_SYS_CD, DBNAME, TABLENAME, DAI_ETL_ID, COLUMNS)

    def update(
            self,
            SRC_SYS_CD: str="jes", 
            DBNAME: str="jes", 
            TABLENAME: str="f4311", 
            DAI_ETL_ID: str="0", 
            COLUMNS: str="21"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.DBNAME = DBNAME
        self.TABLENAME = TABLENAME
        self.DAI_ETL_ID = DAI_ETL_ID
        self.COLUMNS = COLUMNS
        pass
