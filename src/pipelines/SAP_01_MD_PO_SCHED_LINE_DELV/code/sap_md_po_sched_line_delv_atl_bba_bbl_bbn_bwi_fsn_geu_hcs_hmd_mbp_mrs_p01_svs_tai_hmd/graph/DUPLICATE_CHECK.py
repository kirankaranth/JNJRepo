from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("PO_NUM"), 
        col("PO_LINE_NBR"), 
        col("DELV_SCHED_CNT_NBR"), 
        col("ORDER_TYPE"), 
        col("ORDER_CO"), 
        col("ORDER_SUF")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
