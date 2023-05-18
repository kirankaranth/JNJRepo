from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            sourceDatabase: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            ACT_PUSH: str=None,
            MTS_MTO_FL: str=None,
            DUAL_SRCNG_WIP: str=None,
            CNSMPTN_MODE: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            MANDT, 
            sourceDatabase, 
            sourceTable, 
            DAI_ETL_ID, 
            ConfigDatabase, 
            ACT_PUSH, 
            MTS_MTO_FL, 
            DUAL_SRCNG_WIP, 
            CNSMPTN_MODE
        )

    def update(
            self,
            sourceSystem: str="hm2",
            targetSchema: str="dev_md_l1",
            MANDT: str="100",
            sourceDatabase: str="hm2",
            sourceTable: str="NSDM_V_MARC",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            ACT_PUSH: str="cast(null as string)",
            MTS_MTO_FL: str="cast(null as string)",
            DUAL_SRCNG_WIP: str="cast(null as string)",
            CNSMPTN_MODE: str="cast(null as string)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.sourceDatabase = sourceDatabase
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.ACT_PUSH = ACT_PUSH
        self.MTS_MTO_FL = MTS_MTO_FL
        self.DUAL_SRCNG_WIP = DUAL_SRCNG_WIP
        self.CNSMPTN_MODE = CNSMPTN_MODE
        pass
