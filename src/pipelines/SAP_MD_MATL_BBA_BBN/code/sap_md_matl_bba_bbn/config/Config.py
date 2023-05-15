from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            configDatabase: str=None,
            MANDT: str=None,
            DBTABLE1: str=None,
            DBTABLE2: str=None,
            DAI_ETL_ID: int=None,
            DBTABLE3: str=None,
            DBTABLE4: str=None,
            DBTABLE5: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            sourceDatabase, 
            configDatabase, 
            MANDT, 
            DBTABLE1, 
            DBTABLE2, 
            DAI_ETL_ID, 
            DBTABLE3, 
            DBTABLE4, 
            DBTABLE5
        )

    def update(
            self,
            sourceSystem: str="bba",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="bba",
            configDatabase: str=" ",
            MANDT: str="100",
            DBTABLE1: str="mara",
            DBTABLE2: str="makt",
            DAI_ETL_ID: int=0,
            DBTABLE3: str="ausp",
            DBTABLE4: str="inob",
            DBTABLE5: str="cabn",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.configDatabase = configDatabase
        self.MANDT = MANDT
        self.DBTABLE1 = DBTABLE1
        self.DBTABLE2 = DBTABLE2
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.DBTABLE3 = DBTABLE3
        self.DBTABLE4 = DBTABLE4
        self.DBTABLE5 = DBTABLE5
        pass
