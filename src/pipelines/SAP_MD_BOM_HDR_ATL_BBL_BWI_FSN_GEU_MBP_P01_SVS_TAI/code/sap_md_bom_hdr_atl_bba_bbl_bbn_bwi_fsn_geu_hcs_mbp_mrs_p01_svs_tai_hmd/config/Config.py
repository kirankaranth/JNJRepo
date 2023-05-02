from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            targetSchema: str=None,
            ConfigDatabase: str=None,
            BOM_VLD_TO_DTTM: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, MANDT, sourceDatabase, DAI_ETL_ID, targetSchema, ConfigDatabase, BOM_VLD_TO_DTTM)

    def update(
            self,
            sourceSystem: str="pqa_hm2",
            MANDT: str="100",
            sourceDatabase: str="hm2",
            DAI_ETL_ID: int=0,
            targetSchema: str="dev_md_l1",
            ConfigDatabase: str=" ",
            BOM_VLD_TO_DTTM: str="CAST(NULL AS timestamp)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.targetSchema = targetSchema
        self.ConfigDatabase = ConfigDatabase
        self.BOM_VLD_TO_DTTM = BOM_VLD_TO_DTTM
        pass
