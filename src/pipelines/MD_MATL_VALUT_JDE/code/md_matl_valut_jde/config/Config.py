from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, DBTABLE: str=None, DBTABLE1: str=None, sourceDatabase: str=None, **kwargs):
        self.spark = None
        self.update(DBTABLE, DBTABLE1, sourceDatabase)

    def update(self, DBTABLE: str="f4105", DBTABLE1: str="f41021", sourceDatabase: str="gmd", **kwargs):
        prophecy_spark = self.spark
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.sourceDatabase = sourceDatabase
        pass
