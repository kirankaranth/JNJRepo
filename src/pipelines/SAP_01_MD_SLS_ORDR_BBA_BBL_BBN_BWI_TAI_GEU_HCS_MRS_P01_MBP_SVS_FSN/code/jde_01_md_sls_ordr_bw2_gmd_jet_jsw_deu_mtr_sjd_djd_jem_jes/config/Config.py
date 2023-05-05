from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            sourceDatabase: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            ConfigDatabase: str=None,
            FORBID_SLS_IN: str=None,
            CLS_OF_TRD: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            sourceDatabase, 
            targetSchema, 
            MANDT, 
            DAI_ETL_ID, 
            sourceTable, 
            ConfigDatabase, 
            FORBID_SLS_IN, 
            CLS_OF_TRD
        )

    def update(
            self,
            sourceSystem: str="bbn",
            sourceDatabase: str="bbn",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            DAI_ETL_ID: int=0,
            sourceTable: str="VBAK",
            ConfigDatabase: str=" ",
            FORBID_SLS_IN: str="cast( NULL as STRING)",
            CLS_OF_TRD: str="cast( NULL as STRING)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.sourceDatabase = sourceDatabase
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.ConfigDatabase = ConfigDatabase
        self.FORBID_SLS_IN = FORBID_SLS_IN
        self.CLS_OF_TRD = CLS_OF_TRD
        pass
