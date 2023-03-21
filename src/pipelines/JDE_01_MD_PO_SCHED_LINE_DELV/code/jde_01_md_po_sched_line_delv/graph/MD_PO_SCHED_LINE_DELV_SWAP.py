from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv.config.ConfigStore import *
from jde_01_md_po_sched_line_delv.udfs.UDFs import *

def MD_PO_SCHED_LINE_DELV_SWAP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", "dbfs:/mnt/dev_curdelta/l1_prophecy/md_po_sched_line_delv_swap")\
        .mode("error")\
        .saveAsTable(f"l1_md_prophecy.md_po_sched_line_delv_swap")
