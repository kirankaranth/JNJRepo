from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, F4101: str=None, F4105: str=None, F41021: str=None, sourceDatabase: str=None, **kwargs):
        self.spark = None
        self.update(F4101, F4105, F41021, sourceDatabase)

    def update(self, F4101: str="f4101", F4105: str="f4105", F41021: str="f41021", sourceDatabase: str="gmd", **kwargs):
        prophecy_spark = self.spark
        self.F4101 = F4101
        self.F4105 = F4105
        self.F41021 = F41021
        self.sourceDatabase = sourceDatabase
        pass
