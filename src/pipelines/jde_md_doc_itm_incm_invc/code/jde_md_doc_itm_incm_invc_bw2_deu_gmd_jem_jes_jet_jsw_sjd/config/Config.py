from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceTable: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            sourceSystem: str=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceTable, sourceDatabase, DAI_ETL_ID, sourceSystem, targetSchema)

    def update(
            self,
            sourceTable: str="f43199",
            sourceDatabase: str="bw2",
            DAI_ETL_ID: int=0,
            sourceSystem: str="bw2",
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceTable = sourceTable
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        pass
