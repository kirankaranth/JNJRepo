from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            targetSchema: str=None,
            ENTRP_DSTN_CHN_STS_CD: str=None,
            MATL_BASE_CD: str=None,
            MATL_SLS_CAT_GRP_DESC: str=None,
            PRC_BND_CAT: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            SRC_SYS_CD, 
            sourceDatabase, 
            sourceSystem, 
            MANDT, 
            DAI_ETL_ID, 
            sourceTable, 
            targetSchema, 
            ENTRP_DSTN_CHN_STS_CD, 
            MATL_BASE_CD, 
            MATL_SLS_CAT_GRP_DESC, 
            PRC_BND_CAT
        )

    def update(
            self,
            SRC_SYS_CD: str="bbl",
            sourceDatabase: str="bbl",
            sourceSystem: str="bbl",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceTable: str="mvke",
            targetSchema: str="dev_md_l1",
            ENTRP_DSTN_CHN_STS_CD: str="null",
            MATL_BASE_CD: str="null",
            MATL_SLS_CAT_GRP_DESC: str="null",
            PRC_BND_CAT: str="trim(PLGTP)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.targetSchema = targetSchema
        self.ENTRP_DSTN_CHN_STS_CD = ENTRP_DSTN_CHN_STS_CD
        self.MATL_BASE_CD = MATL_BASE_CD
        self.MATL_SLS_CAT_GRP_DESC = MATL_SLS_CAT_GRP_DESC
        self.PRC_BND_CAT = PRC_BND_CAT
        pass
