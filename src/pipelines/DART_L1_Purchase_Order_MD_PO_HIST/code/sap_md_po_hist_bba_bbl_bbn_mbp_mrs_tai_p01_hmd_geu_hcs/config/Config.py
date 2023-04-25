from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            configDatabase: str=None,
            MANDT: str=None,
            STK_SGMNT: str=None,
            UOM_FROM_SRVC_ENT_SHT: str=None,
            LOGL_SYS: str=None,
            QTY_IN_PAREL_UNIT_OF_MEAS: str=None,
            GOODS_RCPT_BLOK_STK: str=None,
            TYPE_OF_PAREL_UNIT_OF_MEAS: str=None,
            VAL_GOODS_RCPT_BLOK_STK: str=None,
            SEASN_YR: str=None,
            SEASN: str=None,
            FSHN_CLCT: str=None,
            FSHN_Theme: str=None,
            QTY3: str=None,
            CHAR_VAL_1: str=None,
            CHAR_VAL_2: str=None,
            CHAR_VAL_3: str=None,
            TAX_RPTG_CTRY_REGN: str=None,
            ORIG_OF_AN_INVC_ITM: str=None,
            RETN_AMT_IN_DOC_CRNCY: str=None,
            RETN_AMT_IN_CO_CD_CRNCY: str=None,
            PSTD_RETN_AMT_IN_DOC_CRNCY: str=None,
            PSTD_SCTY_RETN_AMT: str=None,
            DELV: str=None,
            DELV_ITM: str=None,
            EXCH_RT: str=None,
            MLT_ACCT_ASGNMT: str=None,
            GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY: str=None,
            QTY_IN_PO_PRC_UNIT: str=None,
            QTY_IN_VALUT_GR_BLOK_STK: str=None,
            AMT_IN_LCL_CRNCY: str=None,
            EXCH_RT_DIFF_AMT: str=None,
            QTY2: str=None,
            ACC_AT_ORIG: str=None,
            VALUT_GOODS_RCPT_BLOK_STK: str=None,
            AMT_IN_DOC_CRNCY: str=None,
            PCDR_FOR_UPDT_SCHED_LINE_QTY: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            sourceDatabase, 
            targetSchema, 
            sourceTable, 
            DAI_ETL_ID, 
            configDatabase, 
            MANDT, 
            STK_SGMNT, 
            UOM_FROM_SRVC_ENT_SHT, 
            LOGL_SYS, 
            QTY_IN_PAREL_UNIT_OF_MEAS, 
            GOODS_RCPT_BLOK_STK, 
            TYPE_OF_PAREL_UNIT_OF_MEAS, 
            VAL_GOODS_RCPT_BLOK_STK, 
            SEASN_YR, 
            SEASN, 
            FSHN_CLCT, 
            FSHN_Theme, 
            QTY3, 
            CHAR_VAL_1, 
            CHAR_VAL_2, 
            CHAR_VAL_3, 
            TAX_RPTG_CTRY_REGN, 
            ORIG_OF_AN_INVC_ITM, 
            RETN_AMT_IN_DOC_CRNCY, 
            RETN_AMT_IN_CO_CD_CRNCY, 
            PSTD_RETN_AMT_IN_DOC_CRNCY, 
            PSTD_SCTY_RETN_AMT, 
            DELV, 
            DELV_ITM, 
            EXCH_RT, 
            MLT_ACCT_ASGNMT, 
            GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY, 
            QTY_IN_PO_PRC_UNIT, 
            QTY_IN_VALUT_GR_BLOK_STK, 
            AMT_IN_LCL_CRNCY, 
            EXCH_RT_DIFF_AMT, 
            QTY2, 
            ACC_AT_ORIG, 
            VALUT_GOODS_RCPT_BLOK_STK, 
            AMT_IN_DOC_CRNCY, 
            PCDR_FOR_UPDT_SCHED_LINE_QTY
        )

    def update(
            self,
            sourceSystem: str="tai",
            sourceDatabase: str="tai",
            targetSchema: str="dev_md_l1",
            sourceTable: str="ekbe",
            DAI_ETL_ID: int=0,
            configDatabase: str=None,
            MANDT: str="100",
            STK_SGMNT: str="CAST(null as string)",
            UOM_FROM_SRVC_ENT_SHT: str="CAST(null as string)",
            LOGL_SYS: str="CAST(null as string)",
            QTY_IN_PAREL_UNIT_OF_MEAS: str="CAST(null as decimal(18,4))",
            GOODS_RCPT_BLOK_STK: str="CAST(null as decimal(18,4))",
            TYPE_OF_PAREL_UNIT_OF_MEAS: str="CAST(null as string)",
            VAL_GOODS_RCPT_BLOK_STK: str="CAST(null as decimal(18,4))",
            SEASN_YR: str="CAST(null as string)",
            SEASN: str="CAST(null as string)",
            FSHN_CLCT: str="CAST(null as string)",
            FSHN_Theme: str="CAST(null as string)",
            QTY3: str="CAST(null as decimal(18,4))",
            CHAR_VAL_1: str="CAST(null as string)",
            CHAR_VAL_2: str="CAST(null as string)",
            CHAR_VAL_3: str="CAST(null as string)",
            TAX_RPTG_CTRY_REGN: str="CAST(null as string)",
            ORIG_OF_AN_INVC_ITM: str="CAST(null as string)",
            RETN_AMT_IN_DOC_CRNCY: str="CAST(null as decimal(18,4))",
            RETN_AMT_IN_CO_CD_CRNCY: str="CAST(null as decimal(18,4))",
            PSTD_RETN_AMT_IN_DOC_CRNCY: str="CAST(null as decimal(18,4))",
            PSTD_SCTY_RETN_AMT: str="CAST(null as decimal(18,4))",
            DELV: str="CAST(null as string)",
            DELV_ITM: str="CAST(null as string)",
            EXCH_RT: str="CAST(null as decimal(18,4))",
            MLT_ACCT_ASGNMT: str="CAST(null as string)",
            GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY: str="CAST(null as decimal(18,4))",
            QTY_IN_PO_PRC_UNIT: str="CAST(null as decimal(18,4))",
            QTY_IN_VALUT_GR_BLOK_STK: str="CAST(null as decimal(18,4))",
            AMT_IN_LCL_CRNCY: str="CAST(null as decimal(18,4))",
            EXCH_RT_DIFF_AMT: str="CAST(null as decimal(18,4))",
            QTY2: str="CAST(null as decimal(18,4))",
            ACC_AT_ORIG: str="CAST(null as string)",
            VALUT_GOODS_RCPT_BLOK_STK: str="CAST(null as decimal(18,4))",
            AMT_IN_DOC_CRNCY: str="CAST(null as decimal(18,4))",
            PCDR_FOR_UPDT_SCHED_LINE_QTY: str="trim(et_upd)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.configDatabase = configDatabase
        self.MANDT = MANDT
        self.STK_SGMNT = STK_SGMNT
        self.UOM_FROM_SRVC_ENT_SHT = UOM_FROM_SRVC_ENT_SHT
        self.LOGL_SYS = LOGL_SYS
        self.QTY_IN_PAREL_UNIT_OF_MEAS = QTY_IN_PAREL_UNIT_OF_MEAS
        self.GOODS_RCPT_BLOK_STK = GOODS_RCPT_BLOK_STK
        self.TYPE_OF_PAREL_UNIT_OF_MEAS = TYPE_OF_PAREL_UNIT_OF_MEAS
        self.VAL_GOODS_RCPT_BLOK_STK = VAL_GOODS_RCPT_BLOK_STK
        self.SEASN_YR = SEASN_YR
        self.SEASN = SEASN
        self.FSHN_CLCT = FSHN_CLCT
        self.FSHN_Theme = FSHN_Theme
        self.QTY3 = QTY3
        self.CHAR_VAL_1 = CHAR_VAL_1
        self.CHAR_VAL_2 = CHAR_VAL_2
        self.CHAR_VAL_3 = CHAR_VAL_3
        self.TAX_RPTG_CTRY_REGN = TAX_RPTG_CTRY_REGN
        self.ORIG_OF_AN_INVC_ITM = ORIG_OF_AN_INVC_ITM
        self.RETN_AMT_IN_DOC_CRNCY = RETN_AMT_IN_DOC_CRNCY
        self.RETN_AMT_IN_CO_CD_CRNCY = RETN_AMT_IN_CO_CD_CRNCY
        self.PSTD_RETN_AMT_IN_DOC_CRNCY = PSTD_RETN_AMT_IN_DOC_CRNCY
        self.PSTD_SCTY_RETN_AMT = PSTD_SCTY_RETN_AMT
        self.DELV = DELV
        self.DELV_ITM = DELV_ITM
        self.EXCH_RT = EXCH_RT
        self.MLT_ACCT_ASGNMT = MLT_ACCT_ASGNMT
        self.GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY = GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY
        self.QTY_IN_PO_PRC_UNIT = QTY_IN_PO_PRC_UNIT
        self.QTY_IN_VALUT_GR_BLOK_STK = QTY_IN_VALUT_GR_BLOK_STK
        self.AMT_IN_LCL_CRNCY = AMT_IN_LCL_CRNCY
        self.EXCH_RT_DIFF_AMT = EXCH_RT_DIFF_AMT
        self.QTY2 = QTY2
        self.ACC_AT_ORIG = ACC_AT_ORIG
        self.VALUT_GOODS_RCPT_BLOK_STK = VALUT_GOODS_RCPT_BLOK_STK
        self.AMT_IN_DOC_CRNCY = AMT_IN_DOC_CRNCY
        self.PCDR_FOR_UPDT_SCHED_LINE_QTY = PCDR_FOR_UPDT_SCHED_LINE_QTY
        pass
