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
            IN_DOC_PSTD_PREV_PER
        )

    def update(
            self,
            sourceTable: str="RSEG",
            sourceDatabase: str="hm2",
            MANDT: str="500",
            DAI_ETL_ID: int=0,
            sourceSystem: str="HM2",
            targetSchema: str="dev_md_l1",
            TAX_RDCTN_FOR_RETN: str="trim(XRETTAXNET)",
            LIFO_FIFO_RLVNT: str="trim(XLIFO)",
            IN_DOC_PSTD_PREV_PER: str="trim(XRUEB)",
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
        pass
