from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            DBNAME: str=None,
            DBTABLE1: str=None,
            DBTABLE2: str=None,
            DBTABLE3: str=None,
            DBTABLE4: str=None,
            COLUMNS: int=None,
            DAI_ETL_ID: int=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, DBNAME, DBTABLE1, DBTABLE2, DBTABLE3, DBTABLE4, COLUMNS, DAI_ETL_ID)

    def update(
            self,
            sourceSystem: str="hm2",
            targetSchema: str="dev_md_l1",
            DBNAME: str="hm2",
            DBTABLE1: str="nsdm_v_mard",
            DBTABLE2: str="nsdm_v_mchb",
            DBTABLE3: str="nsdm_v_msku",
            DBTABLE4: str="mara",
            COLUMNS: int=30,
            DAI_ETL_ID: int=0,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.DBNAME = DBNAME
        self.DBTABLE1 = DBTABLE1
        self.DBTABLE2 = DBTABLE2
        self.DBTABLE3 = DBTABLE3
        self.DBTABLE4 = DBTABLE4
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        pass
