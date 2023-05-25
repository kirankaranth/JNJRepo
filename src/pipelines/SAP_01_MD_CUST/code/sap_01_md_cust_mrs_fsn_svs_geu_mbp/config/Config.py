from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            BUSN_PRPS_CMPLT_FL: str=None,
            SUFRAMA_CD: str=None,
            RG_NUM: str=None,
            ISS_BY: str=None,
            ST: str=None,
            RG_ISU_DTTM: str=None,
            RIC_NUM: str=None,
            FRGN_NATL_REGS: str=None,
            RNE_ISU_DTTM: str=None,
            CNAE: str=None,
            LEGAL_NATR: str=None,
            CRT_NUM: str=None,
            ICMS_TAXPY: str=None,
            INDSTR_MN_TYPE: str=None,
            TAX_DCLTN_TYPE: str=None,
            CO_SIZE: str=None,
            DCLTN_RGMN_PIS_COFINS: str=None,
            FEE_SCHED: str=None,
            DUNS_NUM: str=None,
            DUNS_4: str=None,
            TAX_NUM5: str=None,
            TAX_NUM_6: str=None,
            MAX_STCK_HGHT_PKGNG_MATL: str=None,
            UNIT_LGTH_PKGNG_MATL: str=None,
            CUST_RLTD_PACK_EA_PKGNG_MATL: str=None,
            PKGNG_MATL_CUST_VSO: str=None,
            NUM_LYR_UND_INTER_PLLT: str=None,
            PACK_ONLY_ONE_PKG_TYPE_EA_PKM: str=None,
            SIDE_PREF_LD_UNLD: str=None,
            FRN_BK_PREF_LD_UNLD: str=None,
            COLL_UNLD_PT_VSO: str=None,
            PACK_MATL_SPEC_EA_PKGNG_MATL: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            MANDT, 
            sourceDatabase, 
            DAI_ETL_ID, 
            ConfigDatabase, 
            BUSN_PRPS_CMPLT_FL, 
            SUFRAMA_CD, 
            RG_NUM, 
            ISS_BY, 
            ST, 
            RG_ISU_DTTM, 
            RIC_NUM, 
            FRGN_NATL_REGS, 
            RNE_ISU_DTTM, 
            CNAE, 
            LEGAL_NATR, 
            CRT_NUM, 
            ICMS_TAXPY, 
            INDSTR_MN_TYPE, 
            TAX_DCLTN_TYPE, 
            CO_SIZE, 
            DCLTN_RGMN_PIS_COFINS, 
            FEE_SCHED, 
            DUNS_NUM, 
            DUNS_4, 
            TAX_NUM5, 
            TAX_NUM_6, 
            MAX_STCK_HGHT_PKGNG_MATL, 
            UNIT_LGTH_PKGNG_MATL, 
            CUST_RLTD_PACK_EA_PKGNG_MATL, 
            PKGNG_MATL_CUST_VSO, 
            NUM_LYR_UND_INTER_PLLT, 
            PACK_ONLY_ONE_PKG_TYPE_EA_PKM, 
            SIDE_PREF_LD_UNLD, 
            FRN_BK_PREF_LD_UNLD, 
            COLL_UNLD_PT_VSO, 
            PACK_MATL_SPEC_EA_PKGNG_MATL
        )

    def update(
            self,
            sourceSystem: str="mrs",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="mrs",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            BUSN_PRPS_CMPLT_FL: str="CAST(null as string)",
            SUFRAMA_CD: str="CAST(null as string)",
            RG_NUM: str="CAST(null as string)",
            ISS_BY: str="CAST(null as string)",
            ST: str="CAST(null as string)",
            RG_ISU_DTTM: str="to_timestamp(null)",
            RIC_NUM: str="CAST(null as string)",
            FRGN_NATL_REGS: str="CAST(null as string)",
            RNE_ISU_DTTM: str="to_timestamp(null)",
            CNAE: str="CAST(null as string)",
            LEGAL_NATR: str="CAST(null as string)",
            CRT_NUM: str="CAST(null as string)",
            ICMS_TAXPY: str="CAST(null as string)",
            INDSTR_MN_TYPE: str="CAST(null as string)",
            TAX_DCLTN_TYPE: str="CAST(null as string)",
            CO_SIZE: str="CAST(null as string)",
            DCLTN_RGMN_PIS_COFINS: str="CAST(null as string)",
            FEE_SCHED: str="CAST(null as string)",
            DUNS_NUM: str="CAST(null as string)",
            DUNS_4: str="CAST(null as string)",
            TAX_NUM5: str="CAST(null as string)",
            TAX_NUM_6: str="CAST(null as string)",
            MAX_STCK_HGHT_PKGNG_MATL: str="CAST(TRIM(_VSO_R_PALHGT) as Decimal (18,4))",
            UNIT_LGTH_PKGNG_MATL: str="TRIM(_VSO_R_PAL_UL)",
            CUST_RLTD_PACK_EA_PKGNG_MATL: str="TRIM(_VSO_R_PK_MAT)",
            PKGNG_MATL_CUST_VSO: str="TRIM(_VSO_R_MATPAL)",
            NUM_LYR_UND_INTER_PLLT: str="TRIM(_VSO_R_I_NO_LYR)",
            PACK_ONLY_ONE_PKG_TYPE_EA_PKM: str="trim(_VSO_R_ONE_SORT)",
            SIDE_PREF_LD_UNLD: str="trim(_VSO_R_ULD_SIDE)",
            FRN_BK_PREF_LD_UNLD: str="trim(_VSO_R_LOAD_PREF)",
            COLL_UNLD_PT_VSO: str="trim(_VSO_R_DPOINT)",
            PACK_MATL_SPEC_EA_PKGNG_MATL: str="trim(_VSO_R_ONE_MAT)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.BUSN_PRPS_CMPLT_FL = BUSN_PRPS_CMPLT_FL
        self.SUFRAMA_CD = SUFRAMA_CD
        self.RG_NUM = RG_NUM
        self.ISS_BY = ISS_BY
        self.ST = ST
        self.RG_ISU_DTTM = RG_ISU_DTTM
        self.RIC_NUM = RIC_NUM
        self.FRGN_NATL_REGS = FRGN_NATL_REGS
        self.RNE_ISU_DTTM = RNE_ISU_DTTM
        self.CNAE = CNAE
        self.LEGAL_NATR = LEGAL_NATR
        self.CRT_NUM = CRT_NUM
        self.ICMS_TAXPY = ICMS_TAXPY
        self.INDSTR_MN_TYPE = INDSTR_MN_TYPE
        self.TAX_DCLTN_TYPE = TAX_DCLTN_TYPE
        self.CO_SIZE = CO_SIZE
        self.DCLTN_RGMN_PIS_COFINS = DCLTN_RGMN_PIS_COFINS
        self.FEE_SCHED = FEE_SCHED
        self.DUNS_NUM = DUNS_NUM
        self.DUNS_4 = DUNS_4
        self.TAX_NUM5 = TAX_NUM5
        self.TAX_NUM_6 = TAX_NUM_6
        self.MAX_STCK_HGHT_PKGNG_MATL = MAX_STCK_HGHT_PKGNG_MATL
        self.UNIT_LGTH_PKGNG_MATL = UNIT_LGTH_PKGNG_MATL
        self.CUST_RLTD_PACK_EA_PKGNG_MATL = CUST_RLTD_PACK_EA_PKGNG_MATL
        self.PKGNG_MATL_CUST_VSO = PKGNG_MATL_CUST_VSO
        self.NUM_LYR_UND_INTER_PLLT = NUM_LYR_UND_INTER_PLLT
        self.PACK_ONLY_ONE_PKG_TYPE_EA_PKM = PACK_ONLY_ONE_PKG_TYPE_EA_PKM
        self.SIDE_PREF_LD_UNLD = SIDE_PREF_LD_UNLD
        self.FRN_BK_PREF_LD_UNLD = FRN_BK_PREF_LD_UNLD
        self.COLL_UNLD_PT_VSO = COLL_UNLD_PT_VSO
        self.PACK_MATL_SPEC_EA_PKGNG_MATL = PACK_MATL_SPEC_EA_PKGNG_MATL
        pass
