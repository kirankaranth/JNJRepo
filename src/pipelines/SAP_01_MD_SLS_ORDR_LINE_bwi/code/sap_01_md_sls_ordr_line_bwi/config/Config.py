from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            nonProdFilter: bool=None,
            nonProdFilterDate: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, MANDT, sourceDatabase, DAI_ETL_ID, targetSchema, nonProdFilter, nonProdFilterDate)

    def update(
            self,
            sourceSystem: str="bwi",
            MANDT: str="400",
            sourceDatabase: str="bwi",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            nonProdFilter: bool=True,
            nonProdFilterDate: str="20230101",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.nonProdFilter = self.get_bool_value(nonProdFilter)
        self.nonProdFilterDate = nonProdFilterDate
        pass
