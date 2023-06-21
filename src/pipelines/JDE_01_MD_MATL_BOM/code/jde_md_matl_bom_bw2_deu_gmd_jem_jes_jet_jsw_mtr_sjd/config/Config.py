from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            ConfigDatabase: str=None,
            sourceTable: str=None,
            DAI_ETL_ID: int=None,
            BOM_NUM: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, targetSchema, sourceDatabase, ConfigDatabase, sourceTable, DAI_ETL_ID, BOM_NUM)

    def update(
            self,
            sourceSystem: str="geu",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="geu",
            ConfigDatabase: str=" ",
            sourceTable: str="f3002",
            DAI_ETL_ID: int=0,
            BOM_NUM: str="concat(ixkit,';',ixmmcu,';' ,ixtbm,';',ixbqty,';',ixcoby,';',ixcpnt,';',ixsbnt)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.ConfigDatabase = ConfigDatabase
        self.sourceTable = sourceTable
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.BOM_NUM = BOM_NUM
        pass
