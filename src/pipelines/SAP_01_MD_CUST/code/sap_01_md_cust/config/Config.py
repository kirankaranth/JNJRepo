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
            DESC_INDSTR_KEY: str=None,
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
            DESC_INDSTR_KEY
        )

    def update(
            self,
            sourceSystem: str="mrs",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="mrs",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            BUSN_PRPS_CMPLT_FL: str="CAST(null as String)",
            SUFRAMA_CD: str="CAST(null as String)",
            RG_NUM: str="CAST(null as String)",
            ISS_BY: str="CAST(null as String)",
            ST: str="CAST(null as String)",
            RG_ISU_DTTM: str="CAST(null as String)",
            RIC_NUM: str="CAST(null as String)",
            FRGN_NATL_REGS: str="CAST(null as String)",
            RNE_ISU_DTTM: str="CAST(null as String)",
            CNAE: str="CAST(null as String)",
            LEGAL_NATR: str="CAST(null as String)",
            CRT_NUM: str="CAST(null as String)",
            ICMS_TAXPY: str="CAST(null as String)",
            INDSTR_MN_TYPE: str="CAST(null as String)",
            TAX_DCLTN_TYPE: str="CAST(null as String)",
            CO_SIZE: str="CAST(null as String)",
            DCLTN_RGMN_PIS_COFINS: str="CAST(null as String)",
            FEE_SCHED: str="CAST(null as String)",
            DUNS_NUM: str="CAST(null as String)",
            DUNS_4: str="CAST(null as String)",
            DESC_INDSTR_KEY: str="CAST(null as String)",
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
        self.DESC_INDSTR_KEY = DESC_INDSTR_KEY
        pass
