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
            DATA_FIL_VAL_DATA_AGE_DTTM: str=None,
            SD_UNIQ_DOC_RL_ID: str=None,
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
            DATA_FIL_VAL_DATA_AGE_DTTM, 
            SD_UNIQ_DOC_RL_ID
        )

    def update(
            self,
            sourceSystem: str="bba",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="bba",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            DATA_FIL_VAL_DATA_AGE_DTTM: str="cast(null as string)",
            SD_UNIQ_DOC_RL_ID: str="'#'",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.DATA_FIL_VAL_DATA_AGE_DTTM = DATA_FIL_VAL_DATA_AGE_DTTM
        self.SD_UNIQ_DOC_RL_ID = SD_UNIQ_DOC_RL_ID
        pass
