from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            DBNAME: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, MANDT, DBNAME, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="bbl",
            targetSchema: str="100",
            MANDT: str="100",
            DBNAME: str="bbl",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
