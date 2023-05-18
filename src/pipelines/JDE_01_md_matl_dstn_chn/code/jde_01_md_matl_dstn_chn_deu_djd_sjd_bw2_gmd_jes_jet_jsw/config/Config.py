from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SRC_SYS_CD: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            DAI_ETL_ID: int=None,
            sourceTable: str=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(SRC_SYS_CD, sourceDatabase, sourceSystem, DAI_ETL_ID, sourceTable, targetSchema)

    def update(
            self,
            SRC_SYS_CD: str="deu",
            sourceDatabase: str="deu",
            sourceSystem: str="deu",
            DAI_ETL_ID: int=0,
            sourceTable: str="f4101",
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SRC_SYS_CD = SRC_SYS_CD
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceTable = sourceTable
        self.targetSchema = targetSchema
        pass
