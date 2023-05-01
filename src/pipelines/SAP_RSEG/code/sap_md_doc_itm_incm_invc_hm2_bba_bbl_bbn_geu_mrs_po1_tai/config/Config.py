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
            BLOK_RSN_ENHNC_FLD
        )

    def update(
            self,
            sourceTable: str="RSEG",
            sourceDatabase: str="bbn",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceSystem: str="BBN",
            targetSchema: str="dev_md_l1",
            TAX_RDCTN_FOR_RETN: str="trim(XRETTAXNET)",
            LIFO_FIFO_RLVNT: str="trim(XLIFO)",
            IN_DOC_PSTD_PREV_PER: str="trim(XRUEB)",
            CONT_ITM_CAT_PRCHSNG_DOC: str="trim(CONT_PSTYP)",
            BTCH_NUM: str="trim(CHARG)",
            BLOK_RSN_ENHNC_FLD: str="trim(SPGREXT)",
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
        pass
