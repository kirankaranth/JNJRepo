from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, CATALOG: str=None, DATABASE: str=None, TABLE: str=None):
        self.spark = None
        self.update(CATALOG, DATABASE, TABLE)

    def update(
            self,
            CATALOG: str="hive_metastore", 
            DATABASE: str="l1_md_prophecy", 
            TABLE: str="MD_SER_NUM_STOCK_SGMNT"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.CATALOG = CATALOG
        self.DATABASE = DATABASE
        self.TABLE = TABLE
        pass
