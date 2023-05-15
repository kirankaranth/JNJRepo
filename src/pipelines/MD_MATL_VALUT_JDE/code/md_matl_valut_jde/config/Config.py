from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            F4101: str=None,
            F4105: str=None,
            F41021: str=None,
            divisor: str=None,
            costDivisor: str=None,
            DAI_ETL_ID: str=None,
            configDatabase: str=None,
            targetSchema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(sourceSystem, F4101, F4105, F41021, divisor, costDivisor, DAI_ETL_ID, configDatabase, targetSchema)

    def update(
            self,
            sourceSystem: str="gmd",
            F4101: str="f4101",
            F4105: str="f4105",
            F41021: str="f41021",
            divisor: str="1",
            costDivisor: str="10000",
            DAI_ETL_ID: str="0",
            configDatabase: str="  ",
            targetSchema: str="dev_md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.F4101 = F4101
        self.F4105 = F4105
        self.F41021 = F41021
        self.divisor = divisor
        self.costDivisor = costDivisor
        self.DAI_ETL_ID = DAI_ETL_ID
        self.configDatabase = configDatabase
        self.targetSchema = targetSchema
        pass
