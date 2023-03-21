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
            BOM_VLD_TO_DTTM: str=None
    ):
        self.spark = None
        self.update(SRC_SYS_CD, MANDT, COLUMNS, DBNAME, DAI_ETL_ID, BOM_VLD_TO_DTTM)

    def update(
            self,
            SRC_SYS_CD: str="bbn", 
            MANDT: str="100", 
            COLUMNS: int=19, 
            DBNAME: str="bbn", 
            DAI_ETL_ID: int=0, 
            BOM_VLD_TO_DTTM: str="to_timestamp(CAST(NULL AS string),'yyyyMMdd')"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.BOM_VLD_TO_DTTM = BOM_VLD_TO_DTTM
        pass
