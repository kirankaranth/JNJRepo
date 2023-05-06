from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, sourceDatabase: str=None, DBTABLE: str=None, DBTABLE1: str=None, MANDT: str=None, **kwargs):
        self.spark = None
        self.update(sourceDatabase, DBTABLE, DBTABLE1, MANDT)

    def update(self, sourceDatabase: str="bba", DBTABLE: str="MBEW", DBTABLE1: str="MARA", MANDT: str="100", **kwargs):
        prophecy_spark = self.spark
        self.sourceDatabase = sourceDatabase
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.MANDT = MANDT
        pass
