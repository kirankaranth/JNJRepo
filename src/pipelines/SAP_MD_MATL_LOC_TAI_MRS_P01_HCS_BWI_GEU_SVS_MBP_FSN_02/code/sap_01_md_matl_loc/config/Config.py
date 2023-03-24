from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            MANDT: str=None,
            COLUMNS: int=None,
            targetSchema: str=None,
            TABLENAME: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, MANDT, COLUMNS, targetSchema, TABLENAME, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="bbl",
            MANDT: str="100",
            COLUMNS: int=134,
            targetSchema: str="l1_md_prophecy",
            TABLENAME: str="marc",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.targetSchema = targetSchema
        self.TABLENAME = TABLENAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
