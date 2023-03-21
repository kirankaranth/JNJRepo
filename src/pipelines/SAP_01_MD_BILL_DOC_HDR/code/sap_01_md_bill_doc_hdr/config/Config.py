from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            MANDT: str=None, 
            COLUMNS: int=None, 
            DBNAME: str=None, 
            DAI_ETL_ID: int=None, 
            SHIP_TO: str=None, 
            PSTNG_BILL_STS_CD: str=None, 
            INVC_LIST_STS_CD: str=None
    ):
        self.spark = None
        self.update(SRC_SYS_CD, MANDT, COLUMNS, DBNAME, DAI_ETL_ID, SHIP_TO, PSTNG_BILL_STS_CD, INVC_LIST_STS_CD)

    def update(
            self,
            SRC_SYS_CD: str="bba", 
            MANDT: str="100", 
            COLUMNS: int=80, 
            DBNAME: str="bba", 
            DAI_ETL_ID: int=0, 
            SHIP_TO: str="CAST(NULL\u00A0AS\u00A0STRING)", 
            PSTNG_BILL_STS_CD: str="CAST(NULL\u00A0AS\u00A0STRING)", 
            INVC_LIST_STS_CD: str="CAST(NULL\u00A0AS\u00A0STRING)"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.SHIP_TO = SHIP_TO
        self.PSTNG_BILL_STS_CD = PSTNG_BILL_STS_CD
        self.INVC_LIST_STS_CD = INVC_LIST_STS_CD
        pass
