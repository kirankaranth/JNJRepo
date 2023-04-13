from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *
from dart_l1_purchase_order_md_po_hist.udfs.UDFs import *

def DUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("PO_NUM"), 
        col("PO_LINE_NBR"), 
        col("PO_SEQ_NBR"), 
        col("EV_TYPE_CO"), 
        col("MATL_MVMT_YR"), 
        col("MATL_MVMT_NUM"), 
        col("MATL_MVMT_SEQ_NBR"), 
        col("UNIQ_KEY_ID")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
