from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def MD_SER_NUM_STOCK_SGMNT_SWAP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", "dbfs:/mnt/dev_curdelta/l1_prophecy/md_equipment/md_ser_num_stock_sgmnt_swap")\
        .mode("append")\
        .saveAsTable(f"l1_md_prophecy.md_ser_num_stock_sgmnt_swap")
