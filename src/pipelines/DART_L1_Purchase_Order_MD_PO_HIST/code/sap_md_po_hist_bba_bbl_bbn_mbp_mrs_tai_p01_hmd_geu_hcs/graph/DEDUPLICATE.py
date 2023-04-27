from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.config.ConfigStore import *
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy(
              "SRC_SYS_CD", 
              "PO_NUM", 
              "PO_LINE_NBR", 
              "PO_SEQ_NBR", 
              "EV_TYPE_CO", 
              "MATL_MVMT_YR", 
              "MATL_MVMT_NUM", 
              "MATL_MVMT_SEQ_NBR", 
              "UNIQ_KEY_ID"
            )\
            .orderBy(lit(1))\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
