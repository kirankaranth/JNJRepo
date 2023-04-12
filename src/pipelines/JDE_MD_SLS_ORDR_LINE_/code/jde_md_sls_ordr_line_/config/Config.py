from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            DBNAME: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, DBNAME, DBTABLE, DBTABLE1, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="jet",
            targetSchema: str="dev_md_l1",
            DBNAME: str="jet",
            DBTABLE: str="f42119",
            DBTABLE1: str="f4211",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.DBNAME = DBNAME
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass