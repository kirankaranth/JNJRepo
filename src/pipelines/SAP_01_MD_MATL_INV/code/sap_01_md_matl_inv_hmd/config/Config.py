from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            configDatabase: str=None,
            MANDT: str=None,
            NSDM_V_MARD: str=None,
            NSDM_V_MCHB: str=None,
            NSDM_V_MSKU: str=None,
            MARA: str=None,
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
            NSDM_V_MARD, 
            NSDM_V_MCHB, 
            NSDM_V_MSKU, 
            MARA, 
            DAI_ETL_ID
        )

    def update(
            self,
            sourceSystem: str="hm2",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="hm2",
            configDatabase: str=" ",
            MANDT: str="100",
            NSDM_V_MARD: str="NSDM_V_MARD",
            NSDM_V_MCHB: str="NSDM_V_MCHB",
            NSDM_V_MSKU: str="NSDM_V_MSKU",
            MARA: str="MARA",
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.configDatabase = configDatabase
        self.MANDT = MANDT
        self.NSDM_V_MARD = NSDM_V_MARD
        self.NSDM_V_MCHB = NSDM_V_MCHB
        self.NSDM_V_MSKU = NSDM_V_MSKU
        self.MARA = MARA
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
