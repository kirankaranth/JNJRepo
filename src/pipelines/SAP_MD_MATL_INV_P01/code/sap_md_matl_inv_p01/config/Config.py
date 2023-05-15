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
            DBTABLE3: str=None,
            DBTABLE4: str=None,
            DBTABLE5: str=None,
            DBTABLE6: str=None,
            DBTABLE7: str=None,
            DAI_ETL_ID: int=None,
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
            DBTABLE3, 
            DBTABLE4, 
            DBTABLE5, 
            DBTABLE6, 
            DBTABLE7, 
            DAI_ETL_ID
        )

    def update(
            self,
            sourceSystem: str="p01",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="p01",
            configDatabase: str=" ",
            MANDT: str="020",
            DBTABLE1: str="mard",
            DBTABLE2: str="mchb",
            DBTABLE3: str="msku",
            DBTABLE4: str="mkol",
            DBTABLE5: str="mslb",
            DBTABLE6: str="mssl",
            DBTABLE7: str="mara",
            DAI_ETL_ID: int=0,
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
        self.DBTABLE3 = DBTABLE3
        self.DBTABLE4 = DBTABLE4
        self.DBTABLE5 = DBTABLE5
        self.DBTABLE6 = DBTABLE6
        self.DBTABLE7 = DBTABLE7
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
