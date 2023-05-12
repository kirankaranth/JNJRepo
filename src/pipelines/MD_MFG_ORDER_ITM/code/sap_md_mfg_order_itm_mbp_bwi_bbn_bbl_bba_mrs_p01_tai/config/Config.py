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
            STK_SGMNT: str=None,
            CUST_NUM1: str=None,
            SEASN_YR: str=None,
            SEASN: str=None,
            FSHN_CLCT: str=None,
            FSHN_THEME: str=None,
            ALC_STK_QTY: str=None,
            NUM_OF_ORIG_ORDR: str=None,
            CNFRM_QTY_FOR_ITM: str=None,
            ITM_SEQ: str=None,
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
            STK_SGMNT, 
            CUST_NUM1, 
            SEASN_YR, 
            SEASN, 
            FSHN_CLCT, 
            FSHN_THEME, 
            ALC_STK_QTY, 
            NUM_OF_ORIG_ORDR, 
            CNFRM_QTY_FOR_ITM, 
            ITM_SEQ
        )

    def update(
            self,
            sourceSystem: str="mbp",
            targetSchema: str="dev_md_l1",
            MANDT: str="600",
            sourceDatabase: str="mbp",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            STK_SGMNT: str="CAST(null as String)",
            CUST_NUM1: str="CAST(null as String)",
            SEASN_YR: str="CAST(null as String)",
            SEASN: str="CAST(null as String)",
            FSHN_CLCT: str="CAST(null as String)",
            FSHN_THEME: str="CAST(null as String)",
            ALC_STK_QTY: str="CAST(null as Decimal(18,4))",
            NUM_OF_ORIG_ORDR: str="CAST(null as String)",
            CNFRM_QTY_FOR_ITM: str="CAST(null as Decimal(18,4))",
            ITM_SEQ: str="CAST(null as String)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.STK_SGMNT = STK_SGMNT
        self.CUST_NUM1 = CUST_NUM1
        self.SEASN_YR = SEASN_YR
        self.SEASN = SEASN
        self.FSHN_CLCT = FSHN_CLCT
        self.FSHN_THEME = FSHN_THEME
        self.ALC_STK_QTY = ALC_STK_QTY
        self.NUM_OF_ORIG_ORDR = NUM_OF_ORIG_ORDR
        self.CNFRM_QTY_FOR_ITM = CNFRM_QTY_FOR_ITM
        self.ITM_SEQ = ITM_SEQ
        pass
