from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.config.ConfigStore import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.udfs.UDFs import *

def GET_DUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("PO_NUM"), 
        col("PO_LINE_NBR"), 
        col("ORDER_TYPE"), 
        col("ORDER_CO"), 
        col("ORDER_SUF")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
