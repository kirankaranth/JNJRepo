from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            ENTRP_DSTN_CHN_STS_CD: str=None,
            MATL_BASE_CD: str=None,
            MATL_SLS_CAT_GRP_DESC: str=None,
            PROD_HIER_LVL_NUM: str=None,
            DSTN_CHN_STS_CD_DESC: str=None,
            BLOK_FOR_SLS_ORDR: str=None,
            PRC_BND_CAT: str=None,
            PROD_HIER_DESC: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            SRC_SYS_CD, 
            MANDT, 
            DAI_ETL_ID, 
            sourceTable, 
            targetSchema, 
            sourceDatabase, 
            ENTRP_DSTN_CHN_STS_CD, 
            MATL_BASE_CD, 
            MATL_SLS_CAT_GRP_DESC, 
            PROD_HIER_LVL_NUM, 
            DSTN_CHN_STS_CD_DESC, 
            BLOK_FOR_SLS_ORDR, 
            PRC_BND_CAT, 
            PROD_HIER_DESC
        )

    def update(
            self,
            SRC_SYS_CD: str="bbl",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceTable: str="mvke",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="bbl",
            ENTRP_DSTN_CHN_STS_CD: str="null",
            MATL_BASE_CD: str="null",
            MATL_SLS_CAT_GRP_DESC: str="null",
            PROD_HIER_LVL_NUM: str="TRIM(T179.STUFE)",
            DSTN_CHN_STS_CD_DESC: str="TRIM(TVMST.VMSTB)",
            BLOK_FOR_SLS_ORDR: str="TRIM(TVMS.SPVBC)",
            PRC_BND_CAT: str="trim(PLGTP)",
            PROD_HIER_DESC: str="TRIM(T179T.VTEXT)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.ENTRP_DSTN_CHN_STS_CD = ENTRP_DSTN_CHN_STS_CD
        self.MATL_BASE_CD = MATL_BASE_CD
        self.MATL_SLS_CAT_GRP_DESC = MATL_SLS_CAT_GRP_DESC
        self.PROD_HIER_LVL_NUM = PROD_HIER_LVL_NUM
        self.DSTN_CHN_STS_CD_DESC = DSTN_CHN_STS_CD_DESC
        self.BLOK_FOR_SLS_ORDR = BLOK_FOR_SLS_ORDR
        self.PRC_BND_CAT = PRC_BND_CAT
        self.PROD_HIER_DESC = PROD_HIER_DESC
        pass
