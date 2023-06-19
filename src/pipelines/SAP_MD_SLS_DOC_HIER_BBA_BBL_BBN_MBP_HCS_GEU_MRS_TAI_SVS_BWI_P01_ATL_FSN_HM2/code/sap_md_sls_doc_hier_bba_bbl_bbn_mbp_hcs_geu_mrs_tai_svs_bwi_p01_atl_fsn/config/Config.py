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
            QTY_CONV_CD: str=None,
            MATL_MVMT_YR: str=None,
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
            QTY_CONV_CD, 
            MATL_MVMT_YR
        )

    def update(
            self,
            sourceSystem: str="bba",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="bba",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            QTY_CONV_CD: str="trim(CMETH)",
            MATL_MVMT_YR: str="trim(MJAHR)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.QTY_CONV_CD = QTY_CONV_CD
        self.MATL_MVMT_YR = MATL_MVMT_YR
        pass
