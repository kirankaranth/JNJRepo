from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            configDatabase: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            DAI_ETL_ID: int=None,
            BRAVO_MINOR_DESC_FILTER: str=None,
            B_M_LU_Field: str=None,
            FRAN_CD_FILTER: str=None,
            F_C_LU_Field: str=None,
            MATL_GRP_FILTER: str=None,
            M_G_LU_Field: str=None,
            MATL_TYPE_DESC_FILTER: str=None,
            M_T_D_LU_Field: str=None,
            MATL_TYPE_CD: str=None,
            BRND_CD: str=None,
            FRANCHISE_CD: str=None,
            LCL_PLNG_SUB_FRAN_CD: str=None,
            MATL_GRP_CD: str=None,
            INDSTR_SECTR_CD: str=None,
            MIN_SHLF_RMN_LIF_DAYS_CNT: str=None,
            DSTN_CHN_STS_CD: str=None,
            PROD_HIER_CD: str=None,
            STRG_CONDS_CD: str=None,
            BTCH_MNG_IND: str=None,
            MATL_SHRT_DESC: str=None,
            SRC_SECTR_CD: str=None,
            MATL_PARNT_CD: str=None,
            MATL_SUB_TYPE_CD: str=None,
            FIN_HIER_BASE_CD: str=None,
            IMPLNT_INSTM_IND: str=None,
            KIT_IND: str=None,
            DIR_PART_MRKNG_CD: str=None,
            MATL_CAT_GRP_CD: str=None,
            PLNG_HIER3_CD: str=None,
            MATL_SPEC_NUM: str=None,
            MATL_SPEC_VERS_NUM: str=None,
            VOL_UNIT: str=None,
            VOL: str=None,
            EAN_UPC: str=None,
            MAIN_STRG_LOC: str=None,
            PROD_LINE: str=None,
            MAKE_BUY_IN: str=None,
            TYPE_OF_MATERIAL: str=None,
            STERILE: str=None,
            BRAVO_MINOR_CODE: str=None,
            CMMDTY: str=None,
            MATL_GRP_DESC_2_FILTER: str=None,
            M_G_2_Field: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            targetSchema, 
            sourceDatabase, 
            sourceSystem, 
            configDatabase, 
            DBTABLE, 
            DBTABLE1, 
            DAI_ETL_ID, 
            BRAVO_MINOR_DESC_FILTER, 
            B_M_LU_Field, 
            FRAN_CD_FILTER, 
            F_C_LU_Field, 
            MATL_GRP_FILTER, 
            M_G_LU_Field, 
            MATL_TYPE_DESC_FILTER, 
            M_T_D_LU_Field, 
            MATL_TYPE_CD, 
            BRND_CD, 
            FRANCHISE_CD, 
            LCL_PLNG_SUB_FRAN_CD, 
            MATL_GRP_CD, 
            INDSTR_SECTR_CD, 
            MIN_SHLF_RMN_LIF_DAYS_CNT, 
            DSTN_CHN_STS_CD, 
            PROD_HIER_CD, 
            STRG_CONDS_CD, 
            BTCH_MNG_IND, 
            MATL_SHRT_DESC, 
            SRC_SECTR_CD, 
            MATL_PARNT_CD, 
            MATL_SUB_TYPE_CD, 
            FIN_HIER_BASE_CD, 
            IMPLNT_INSTM_IND, 
            KIT_IND, 
            DIR_PART_MRKNG_CD, 
            MATL_CAT_GRP_CD, 
            PLNG_HIER3_CD, 
            MATL_SPEC_NUM, 
            MATL_SPEC_VERS_NUM, 
            VOL_UNIT, 
            VOL, 
            EAN_UPC, 
            MAIN_STRG_LOC, 
            PROD_LINE, 
            MAKE_BUY_IN, 
            TYPE_OF_MATERIAL, 
            STERILE, 
            BRAVO_MINOR_CODE, 
            CMMDTY, 
            MATL_GRP_DESC_2_FILTER, 
            M_G_2_Field
        )

    def update(
            self,
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="jet",
            sourceSystem: str="jet",
            configDatabase: str=" ",
            DBTABLE: str="f4101_adt",
            DBTABLE1: str="f0005",
            DAI_ETL_ID: int=0,
            BRAVO_MINOR_DESC_FILTER: str="10",
            B_M_LU_Field: str="IMSRP0",
            FRAN_CD_FILTER: str="S1",
            F_C_LU_Field: str="IMSRP1",
            MATL_GRP_FILTER: str="08",
            M_G_LU_Field: str="IMSRP8",
            MATL_TYPE_DESC_FILTER: str="05",
            M_T_D_LU_Field: str="IMPRP0",
            MATL_TYPE_CD: str="TRIM(IMPRP0)",
            BRND_CD: str="TRIM(IMSRP7)",
            FRANCHISE_CD: str="TRIM(IMSRP1)",
            LCL_PLNG_SUB_FRAN_CD: str="TRIM(IMSRP2)",
            MATL_GRP_CD: str="TRIM(IMSRP8)",
            INDSTR_SECTR_CD: str="null",
            MIN_SHLF_RMN_LIF_DAYS_CNT: str="null",
            DSTN_CHN_STS_CD: str="null",
            PROD_HIER_CD: str="TRIM(IMSRP6)",
            STRG_CONDS_CD: str="TRIM(IMPRP7)",
            BTCH_MNG_IND: str="TRIM(IMSRCE)",
            MATL_SHRT_DESC: str="CASE WHEN Length(Trim(IMDSC2)) = 0 THEN Trim(IMDSC1) ELSE CONCAT(Trim(IMDSC1),';',Trim(IMDSC2)) END",
            SRC_SECTR_CD: str="TRIM(IMSRP5)",
            MATL_PARNT_CD: str="null",
            MATL_SUB_TYPE_CD: str="TRIM(IMSRP9)",
            FIN_HIER_BASE_CD: str="TRIM(IMGLPT)",
            IMPLNT_INSTM_IND: str="null",
            KIT_IND: str="TRIM(IMPRP4)",
            DIR_PART_MRKNG_CD: str="null",
            MATL_CAT_GRP_CD: str="null",
            PLNG_HIER3_CD: str="TRIM(IMPRP9)",
            MATL_SPEC_NUM: str="null",
            MATL_SPEC_VERS_NUM: str="null",
            VOL_UNIT: str="IMVCUD",
            VOL: str="null",
            EAN_UPC: str="null",
            MAIN_STRG_LOC: str="null",
            PROD_LINE: str="TRIM(IMSRP3)",
            MAKE_BUY_IN: str="null",
            TYPE_OF_MATERIAL: str="null",
            STERILE: str="null",
            BRAVO_MINOR_CODE: str="TRIM(IMSRP0)",
            CMMDTY: str="null",
            MATL_GRP_DESC_2_FILTER: str="P4",
            M_G_2_Field: str="IMPRP4",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.configDatabase = configDatabase
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.BRAVO_MINOR_DESC_FILTER = BRAVO_MINOR_DESC_FILTER
        self.B_M_LU_Field = B_M_LU_Field
        self.FRAN_CD_FILTER = FRAN_CD_FILTER
        self.F_C_LU_Field = F_C_LU_Field
        self.MATL_GRP_FILTER = MATL_GRP_FILTER
        self.M_G_LU_Field = M_G_LU_Field
        self.MATL_TYPE_DESC_FILTER = MATL_TYPE_DESC_FILTER
        self.M_T_D_LU_Field = M_T_D_LU_Field
        self.MATL_TYPE_CD = MATL_TYPE_CD
        self.BRND_CD = BRND_CD
        self.FRANCHISE_CD = FRANCHISE_CD
        self.LCL_PLNG_SUB_FRAN_CD = LCL_PLNG_SUB_FRAN_CD
        self.MATL_GRP_CD = MATL_GRP_CD
        self.INDSTR_SECTR_CD = INDSTR_SECTR_CD
        self.MIN_SHLF_RMN_LIF_DAYS_CNT = MIN_SHLF_RMN_LIF_DAYS_CNT
        self.DSTN_CHN_STS_CD = DSTN_CHN_STS_CD
        self.PROD_HIER_CD = PROD_HIER_CD
        self.STRG_CONDS_CD = STRG_CONDS_CD
        self.BTCH_MNG_IND = BTCH_MNG_IND
        self.MATL_SHRT_DESC = MATL_SHRT_DESC
        self.SRC_SECTR_CD = SRC_SECTR_CD
        self.MATL_PARNT_CD = MATL_PARNT_CD
        self.MATL_SUB_TYPE_CD = MATL_SUB_TYPE_CD
        self.FIN_HIER_BASE_CD = FIN_HIER_BASE_CD
        self.IMPLNT_INSTM_IND = IMPLNT_INSTM_IND
        self.KIT_IND = KIT_IND
        self.DIR_PART_MRKNG_CD = DIR_PART_MRKNG_CD
        self.MATL_CAT_GRP_CD = MATL_CAT_GRP_CD
        self.PLNG_HIER3_CD = PLNG_HIER3_CD
        self.MATL_SPEC_NUM = MATL_SPEC_NUM
        self.MATL_SPEC_VERS_NUM = MATL_SPEC_VERS_NUM
        self.VOL_UNIT = VOL_UNIT
        self.VOL = VOL
        self.EAN_UPC = EAN_UPC
        self.MAIN_STRG_LOC = MAIN_STRG_LOC
        self.PROD_LINE = PROD_LINE
        self.MAKE_BUY_IN = MAKE_BUY_IN
        self.TYPE_OF_MATERIAL = TYPE_OF_MATERIAL
        self.STERILE = STERILE
        self.BRAVO_MINOR_CODE = BRAVO_MINOR_CODE
        self.CMMDTY = CMMDTY
        self.MATL_GRP_DESC_2_FILTER = MATL_GRP_DESC_2_FILTER
        self.M_G_2_Field = M_G_2_Field
        pass
