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
            DAI_ETL_ID
        )

    def update(
            self,
            sourceSystem: str="atl",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="atl",
            configDatabase: str=" ",
            MANDT: str="100",
            DBTABLE1: str="mard",
            DBTABLE2: str="mchb",
            DBTABLE3: str="mkol",
            DBTABLE4: str="mslb",
            DBTABLE5: str="mssl",
            DBTABLE6: str="mara",
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
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
