from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None, 
            COLUMNS: int=None, 
            DBNAME: str=None, 
            TABLENAME: str=None, 
            DAI_ETL_ID: int=None, 
            VMI_IND: str=None, 
            MSTR_PLNG_FMLY_CD: str=None, 
            SPL_PRCMT_TYPE_CD: str=None, 
            PRCHSNG_GRP_CD: str=None, 
            VALUT_CAT: str=None, 
            FLLP_MATL: str=None, 
            CNTL_CODE: str=None, 
            MM_DFLT_SUPP_AREA: str=None, 
            MTS_MTO_FL: str=None
    ):
        self.spark = None
        self.update(
            SRC_SYS_CD, 
            COLUMNS, 
            DBNAME, 
            TABLENAME, 
            DAI_ETL_ID, 
            VMI_IND, 
            MSTR_PLNG_FMLY_CD, 
            SPL_PRCMT_TYPE_CD, 
            PRCHSNG_GRP_CD, 
            VALUT_CAT, 
            FLLP_MATL, 
            CNTL_CODE, 
            MM_DFLT_SUPP_AREA, 
            MTS_MTO_FL
        )

    def update(
            self,
            SRC_SYS_CD: str="jes", 
            COLUMNS: int=134, 
            DBNAME: str="jes", 
            TABLENAME: str="f4102_adt", 
            DAI_ETL_ID: int=0, 
            VMI_IND: str="cast(null as string)", 
            MSTR_PLNG_FMLY_CD: str="cast(null as string)", 
            SPL_PRCMT_TYPE_CD: str="cast(null as string)", 
            PRCHSNG_GRP_CD: str="cast(null as string)", 
            VALUT_CAT: str="cast(null as string)", 
            FLLP_MATL: str="cast(null as string)", 
            CNTL_CODE: str="cast(null as string)", 
            MM_DFLT_SUPP_AREA: str="cast(null as string)", 
            MTS_MTO_FL: str="cast(null as string)"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.TABLENAME = TABLENAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.VMI_IND = VMI_IND
        self.MSTR_PLNG_FMLY_CD = MSTR_PLNG_FMLY_CD
        self.SPL_PRCMT_TYPE_CD = SPL_PRCMT_TYPE_CD
        self.PRCHSNG_GRP_CD = PRCHSNG_GRP_CD
        self.VALUT_CAT = VALUT_CAT
        self.FLLP_MATL = FLLP_MATL
        self.CNTL_CODE = CNTL_CODE
        self.MM_DFLT_SUPP_AREA = MM_DFLT_SUPP_AREA
        self.MTS_MTO_FL = MTS_MTO_FL
        pass
