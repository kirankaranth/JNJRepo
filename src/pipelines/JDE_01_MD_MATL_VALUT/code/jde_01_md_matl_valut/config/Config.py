from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            DBNAME: str=None, 
            DBTABLE: str=None, 
            COLUMNS: str=None, 
            DAI_ETL_ID: str=None
    ):
        self.spark = None
        self.update(SRC_SYS_CD, DBNAME, DBTABLE, COLUMNS, DAI_ETL_ID)

    def update(
            self,
            SRC_SYS_CD: str="bw2", 
            DBNAME: str="bw2", 
            DBTABLE: str="f4105_adt", 
            COLUMNS: str="38", 
            DAI_ETL_ID: str="0"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.DBNAME = DBNAME
        self.DBTABLE = DBTABLE
        self.COLUMNS = COLUMNS
        self.DAI_ETL_ID = DAI_ETL_ID
        pass
