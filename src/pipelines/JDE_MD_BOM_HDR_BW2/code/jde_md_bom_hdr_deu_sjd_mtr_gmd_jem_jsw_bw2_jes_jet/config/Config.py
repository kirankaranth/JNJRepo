from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            DBNAME: str=None,
            TBNAME: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, DBNAME, TBNAME, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="bw2",
            targetSchema: str="l1_md_prophecy",
            DBNAME: str="bw2",
            TBNAME: str="f3002_adt",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.DBNAME = DBNAME
        self.TBNAME = TBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
