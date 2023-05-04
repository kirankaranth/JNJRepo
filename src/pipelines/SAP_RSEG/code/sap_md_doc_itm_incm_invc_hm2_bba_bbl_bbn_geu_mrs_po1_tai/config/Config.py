from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceTable: str=None,
            sourceDatabase: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceSystem: str=None,
            targetSchema: str=None,
            TAX_RDCTN_FOR_RETN: str=None,
            LIFO_FIFO_RLVNT: str=None,
            IN_DOC_PSTD_PREV_PER: str=None,
            CONT_ITM_CAT_PRCHSNG_DOC: str=None,
            BTCH_NUM: str=None,
            BLOK_RSN_ENHNC_FLD: str=None,
            UPDT_MLT_ACCT_ASGNMT: str=None,
            CMPNT_RSN_IN_INVC: str=None,
            RETN_AMT_IN_DOC_CRNCY: str=None,
            RETN_IN_PCT: str=None,
            RETN_DUE_DTTM: str=None,
            CASH_LDGR_EXP_REVN_ACCT: str=None,
            NUM_OF_PRIN_PRCH_AGMT: str=None,
            ITM_NUM_PRIN_PRCH_AGMT: str=None,
            CENT_CNTRC: str=None,
            CENT_CNTRC_ITM_NUM: str=None,
            ITM_KEY_FOR_ESOA_MSG: str=None,
            ORIG_INVC_ITM: str=None,
            GRP_CHAR_FOR_INVC_VERIF: str=None,
            IN_FOR_DIFF_INVC: str=None,
            DIFF_AMT: str=None,
            CMMDTY_REPRC_INVC_VERIF: str=None,
            INTRNL_LIC_NUM: str=None,
            ITM_NUM: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceTable, 
            sourceDatabase, 
            MANDT, 
            DAI_ETL_ID, 
            sourceSystem, 
            targetSchema, 
            TAX_RDCTN_FOR_RETN, 
            LIFO_FIFO_RLVNT, 
            IN_DOC_PSTD_PREV_PER, 
            CONT_ITM_CAT_PRCHSNG_DOC, 
            BTCH_NUM, 
            BLOK_RSN_ENHNC_FLD, 
            UPDT_MLT_ACCT_ASGNMT, 
            CMPNT_RSN_IN_INVC, 
            RETN_AMT_IN_DOC_CRNCY, 
            RETN_IN_PCT, 
            RETN_DUE_DTTM, 
            CASH_LDGR_EXP_REVN_ACCT, 
            NUM_OF_PRIN_PRCH_AGMT, 
            ITM_NUM_PRIN_PRCH_AGMT, 
            CENT_CNTRC, 
            CENT_CNTRC_ITM_NUM, 
            ITM_KEY_FOR_ESOA_MSG, 
            ORIG_INVC_ITM, 
            GRP_CHAR_FOR_INVC_VERIF, 
            IN_FOR_DIFF_INVC, 
            DIFF_AMT, 
            CMMDTY_REPRC_INVC_VERIF, 
            INTRNL_LIC_NUM, 
            ITM_NUM
        )

    def update(
            self,
            sourceTable: str="RSEG",
            sourceDatabase: str="bbn",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceSystem: str="bbn",
            targetSchema: str="dev_md_l1",
            TAX_RDCTN_FOR_RETN: str="trim(XRETTAXNET)",
            LIFO_FIFO_RLVNT: str="trim(XLIFO)",
            IN_DOC_PSTD_PREV_PER: str="trim(XRUEB)",
            CONT_ITM_CAT_PRCHSNG_DOC: str="trim(CONT_PSTYP)",
            BTCH_NUM: str="trim(CHARG)",
            BLOK_RSN_ENHNC_FLD: str="trim(SPGREXT)",
            UPDT_MLT_ACCT_ASGNMT: str="trim(XHISTMA)",
            CMPNT_RSN_IN_INVC: str="trim(COMPLAINT_REASON)",
            RETN_AMT_IN_DOC_CRNCY: str="trim(RETAMT_FC)",
            RETN_IN_PCT: str="trim(RETPC)",
            RETN_DUE_DTTM: str="case\r\n  when RETDUEDT = '00000000' then null\r\n  else to_timestamp (RETDUEDT, \"yyyyMMdd\")\r\nend",
            CASH_LDGR_EXP_REVN_ACCT: str="trim(RE_ACCOUNT)",
            NUM_OF_PRIN_PRCH_AGMT: str="trim(ERP_CONTRACT_ID)",
            ITM_NUM_PRIN_PRCH_AGMT: str="trim(ERP_CONTRACT_ITM)",
            CENT_CNTRC: str="trim(SRM_CONTRACT_ID)",
            CENT_CNTRC_ITM_NUM: str="trim(SRM_CONTRACT_ITM)",
            ITM_KEY_FOR_ESOA_MSG: str="trim(SRVMAPKEY)",
            ORIG_INVC_ITM: str="trim(INV_ITM_ORIGIN)",
            GRP_CHAR_FOR_INVC_VERIF: str="trim(INVREL)",
            IN_FOR_DIFF_INVC: str="trim(XDINV)\r\n",
            DIFF_AMT: str="trim(DIFF_AMOUNT)",
            CMMDTY_REPRC_INVC_VERIF: str="trim(XCPRF)",
            INTRNL_LIC_NUM: str="trim(LICNO)",
            ITM_NUM: str="trim(ZEILE)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceTable = sourceTable
        self.sourceDatabase = sourceDatabase
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.TAX_RDCTN_FOR_RETN = TAX_RDCTN_FOR_RETN
        self.LIFO_FIFO_RLVNT = LIFO_FIFO_RLVNT
        self.IN_DOC_PSTD_PREV_PER = IN_DOC_PSTD_PREV_PER
        self.CONT_ITM_CAT_PRCHSNG_DOC = CONT_ITM_CAT_PRCHSNG_DOC
        self.BTCH_NUM = BTCH_NUM
        self.BLOK_RSN_ENHNC_FLD = BLOK_RSN_ENHNC_FLD
        self.UPDT_MLT_ACCT_ASGNMT = UPDT_MLT_ACCT_ASGNMT
        self.CMPNT_RSN_IN_INVC = CMPNT_RSN_IN_INVC
        self.RETN_AMT_IN_DOC_CRNCY = RETN_AMT_IN_DOC_CRNCY
        self.RETN_IN_PCT = RETN_IN_PCT
        self.RETN_DUE_DTTM = RETN_DUE_DTTM
        self.CASH_LDGR_EXP_REVN_ACCT = CASH_LDGR_EXP_REVN_ACCT
        self.NUM_OF_PRIN_PRCH_AGMT = NUM_OF_PRIN_PRCH_AGMT
        self.ITM_NUM_PRIN_PRCH_AGMT = ITM_NUM_PRIN_PRCH_AGMT
        self.CENT_CNTRC = CENT_CNTRC
        self.CENT_CNTRC_ITM_NUM = CENT_CNTRC_ITM_NUM
        self.ITM_KEY_FOR_ESOA_MSG = ITM_KEY_FOR_ESOA_MSG
        self.ORIG_INVC_ITM = ORIG_INVC_ITM
        self.GRP_CHAR_FOR_INVC_VERIF = GRP_CHAR_FOR_INVC_VERIF
        self.IN_FOR_DIFF_INVC = IN_FOR_DIFF_INVC
        self.DIFF_AMT = DIFF_AMT
        self.CMMDTY_REPRC_INVC_VERIF = CMMDTY_REPRC_INVC_VERIF
        self.INTRNL_LIC_NUM = INTRNL_LIC_NUM
        self.ITM_NUM = ITM_NUM
        pass
