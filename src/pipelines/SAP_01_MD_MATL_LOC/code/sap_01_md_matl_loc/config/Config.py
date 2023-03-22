from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            MANDT: str=None, 
            COLUMNS: int=None, 
            DBNAME: str=None, 
            TABLENAME: str=None, 
            DAI_ETL_ID: int=None, 
            MMS_FIN_CLSN_CD: str=None, 
            VMI_IND: str=None, 
            MSTR_PLNG_FMLY_CD: str=None, 
            MTS_MTO_FL: str=None
    ):
        self.spark = None
        self.update(
            SRC_SYS_CD, 
            MANDT, 
            COLUMNS, 
            DBNAME, 
            TABLENAME, 
            DAI_ETL_ID, 
            MMS_FIN_CLSN_CD, 
            VMI_IND, 
            MSTR_PLNG_FMLY_CD, 
            MTS_MTO_FL
        )

    def update(
            self,
            SRC_SYS_CD: str="bbl", 
            MANDT: str="100", 
            COLUMNS: int=134, 
            DBNAME: str="bbl", 
            TABLENAME: str="marc", 
            DAI_ETL_ID: int=0, 
            MMS_FIN_CLSN_CD: str="trim(col(\"zzmmsficlass\")", 
            VMI_IND: str="Trim(zzsmiind)", 
            MSTR_PLNG_FMLY_CD: str="trim(zzmpfamily)", 
            MTS_MTO_FL: str="trim(zzmtomtsind)"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.TABLENAME = TABLENAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.MMS_FIN_CLSN_CD = MMS_FIN_CLSN_CD
        self.VMI_IND = VMI_IND
        self.MSTR_PLNG_FMLY_CD = MSTR_PLNG_FMLY_CD
        self.MTS_MTO_FL = MTS_MTO_FL
        pass
