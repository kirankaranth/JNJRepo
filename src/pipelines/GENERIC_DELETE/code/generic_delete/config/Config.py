from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, DATABASE: str=None, CATALOG: str=None, TABLE: str=None, SRCSYSCD: str=None):
        self.spark = None
        self.update(DATABASE, CATALOG, TABLE, SRCSYSCD)

    def update(
            self,
            DATABASE: str="l1_md_prophecy", 
            CATALOG: str="hive_metastore", 
            TABLE: str="MD_SER_NUM_STOCK_SGMNT_SWAP", 
            SRCSYSCD: str="BBN"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.DATABASE = DATABASE
        self.CATALOG = CATALOG
        self.TABLE = TABLE
        self.SRCSYSCD = SRCSYSCD
        pass
