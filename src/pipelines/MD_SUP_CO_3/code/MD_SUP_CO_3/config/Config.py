from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(targetSchema, sourceSystem, sourceDatabase, MANDT, DAI_ETL_ID)

    def update(
            self,
            targetSchema: str="sbx_md_l1",
            sourceSystem: str="tai",
            sourceDatabase: str="taishan",
            MANDT: str=None,
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
