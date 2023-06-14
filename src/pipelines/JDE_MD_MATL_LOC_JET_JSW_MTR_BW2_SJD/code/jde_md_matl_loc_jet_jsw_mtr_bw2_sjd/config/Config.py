from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            sourceTable1: str=None,
            DAI_ETL_ID: int=None,
            configDatabase: str=None,
            PRCTR_CD: str=None,
            SRC_LIST_IND: str=None,
            LD_GRP_CD: str=None,
            PRDTN_SUPR_CD: str=None,
            SPL_PRCMT_TYPE_CD: str=None,
            PRCMT_TYPE_CD: str=None,
            MRP_TYPE_CD: str=None,
            PLNG_STRTGY_GRP_CD: str=None,
            PLAN_DELV_DAYS_CNT: str=None,
            INHS_PRDTN_DAYS_CNT: str=None,
            PRCHSNG_GRP_CD: str=None,
            VMI_IND: str=None,
            MSTR_PLNG_FMLY_CD: str=None,
            ENTR_PRCMT_TYPE_CD: str=None,
            VALUT_CAT: str=None,
            MRP_PRFL: str=None,
            FLLP_MATL: str=None,
            TOT_REPLN_LT: str=None,
            CMMDTY_CD: str=None,
            MRP_GRP: str=None,
            CNTL_CODE: str=None,
            MM_DFLT_SUPP_AREA: str=None,
            MTS_MTO_FL: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            sourceDatabase, 
            sourceTable, 
            sourceTable1, 
            DAI_ETL_ID, 
            configDatabase, 
            PRCTR_CD, 
            SRC_LIST_IND, 
            LD_GRP_CD, 
            PRDTN_SUPR_CD, 
            SPL_PRCMT_TYPE_CD, 
            PRCMT_TYPE_CD, 
            MRP_TYPE_CD, 
            PLNG_STRTGY_GRP_CD, 
            PLAN_DELV_DAYS_CNT, 
            INHS_PRDTN_DAYS_CNT, 
            PRCHSNG_GRP_CD, 
            VMI_IND, 
            MSTR_PLNG_FMLY_CD, 
            ENTR_PRCMT_TYPE_CD, 
            VALUT_CAT, 
            MRP_PRFL, 
            FLLP_MATL, 
            TOT_REPLN_LT, 
            CMMDTY_CD, 
            MRP_GRP, 
            CNTL_CODE, 
            MM_DFLT_SUPP_AREA, 
            MTS_MTO_FL
        )

    def update(
            self,
            sourceSystem: str="deu",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="deu",
            sourceTable: str="f4102",
            sourceTable1: str="f4101",
            DAI_ETL_ID: int=0,
            configDatabase: str=" ",
            PRCTR_CD: str="cast(null as string)",
            SRC_LIST_IND: str="cast(null as string)",
            LD_GRP_CD: str="cast(null as string)",
            PRDTN_SUPR_CD: str="cast(null as string)",
            SPL_PRCMT_TYPE_CD: str="cast(null as string)",
            PRCMT_TYPE_CD: str="cast(null as string)",
            MRP_TYPE_CD: str="cast(null as string)",
            PLNG_STRTGY_GRP_CD: str="cast(null as string)",
            PLAN_DELV_DAYS_CNT: str="cast(null as decimal(18,4))",
            INHS_PRDTN_DAYS_CNT: str="cast(null as decimal(18,4))",
            PRCHSNG_GRP_CD: str="cast(null as string)",
            VMI_IND: str="cast(null as string)",
            MSTR_PLNG_FMLY_CD: str="cast(null as string)",
            ENTR_PRCMT_TYPE_CD: str="cast(null as string)",
            VALUT_CAT: str="cast(null as string)",
            MRP_PRFL: str="cast(null as string)",
            FLLP_MATL: str="cast(null as string)",
            TOT_REPLN_LT: str="cast(null as decimal(18,4))",
            CMMDTY_CD: str="cast(null as string)",
            MRP_GRP: str="cast(null as string)",
            CNTL_CODE: str="cast(null as string)",
            MM_DFLT_SUPP_AREA: str="cast(null as string)",
            MTS_MTO_FL: str="cast(null as string)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.sourceTable1 = sourceTable1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.configDatabase = configDatabase
        self.PRCTR_CD = PRCTR_CD
        self.SRC_LIST_IND = SRC_LIST_IND
        self.LD_GRP_CD = LD_GRP_CD
        self.PRDTN_SUPR_CD = PRDTN_SUPR_CD
        self.SPL_PRCMT_TYPE_CD = SPL_PRCMT_TYPE_CD
        self.PRCMT_TYPE_CD = PRCMT_TYPE_CD
        self.MRP_TYPE_CD = MRP_TYPE_CD
        self.PLNG_STRTGY_GRP_CD = PLNG_STRTGY_GRP_CD
        self.PLAN_DELV_DAYS_CNT = PLAN_DELV_DAYS_CNT
        self.INHS_PRDTN_DAYS_CNT = INHS_PRDTN_DAYS_CNT
        self.PRCHSNG_GRP_CD = PRCHSNG_GRP_CD
        self.VMI_IND = VMI_IND
        self.MSTR_PLNG_FMLY_CD = MSTR_PLNG_FMLY_CD
        self.ENTR_PRCMT_TYPE_CD = ENTR_PRCMT_TYPE_CD
        self.VALUT_CAT = VALUT_CAT
        self.MRP_PRFL = MRP_PRFL
        self.FLLP_MATL = FLLP_MATL
        self.TOT_REPLN_LT = TOT_REPLN_LT
        self.CMMDTY_CD = CMMDTY_CD
        self.MRP_GRP = MRP_GRP
        self.CNTL_CODE = CNTL_CODE
        self.MM_DFLT_SUPP_AREA = MM_DFLT_SUPP_AREA
        self.MTS_MTO_FL = MTS_MTO_FL
        pass
