from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            MANDT: str=None,
            DBNAME: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, MANDT, DBNAME, DAI_ETL_ID, targetSchema)

    def update(
            self,
            sourceSystem: str="tai",
            MANDT: str="100",
            DBNAME: str="tai",
            DAI_ETL_ID: int=0,
            targetSchema: str="l1_md_prophecy",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        pass
