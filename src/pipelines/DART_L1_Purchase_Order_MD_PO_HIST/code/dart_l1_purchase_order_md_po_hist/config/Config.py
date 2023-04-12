from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: str=None,
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
            CHAR_VAL_3
        )

    def update(
            self,
            sourceSystem: str="tai",
            sourceDatabase: str="tai",
            targetSchema: str="dev_md_l1",
            sourceTable: str="ekbe",
            DAI_ETL_ID: str="0",
            configDatabase: str=None,
            MANDT: str="100",
            STK_SGMNT: str="trim(sgt_scat)",
            UOM_FROM_SRVC_ENT_SHT: str="trim(sesuom)",
            LOGL_SYS: str="trim(LOGL_SYS)",
            QTY_IN_PAREL_UNIT_OF_MEAS: str="trim(_cwm_bamng)",
            GOODS_RCPT_BLOK_STK: str="trim(_cwm_wesbs)",
            TYPE_OF_PAREL_UNIT_OF_MEAS: str="trim(_cwm_ty2tq)",
            VAL_GOODS_RCPT_BLOK_STK: str="trim(_cwm_wesbb)",
            SEASN_YR: str="trim(fsh_season_year)",
            SEASN: str="trim(fsh_season)",
            FSHN_CLCT: str="trim(fsh_collection)",
            FSHN_Theme: str="trim(fsh_theme)",
            QTY3: str="trim(qty_diff)",
            CHAR_VAL_1: str="trim(wrf_charstc1)",
            CHAR_VAL_2: str="trim(wrf_charstc2)",
            CHAR_VAL_3: str="trim(wrf_charstc3)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = DAI_ETL_ID
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
        pass
