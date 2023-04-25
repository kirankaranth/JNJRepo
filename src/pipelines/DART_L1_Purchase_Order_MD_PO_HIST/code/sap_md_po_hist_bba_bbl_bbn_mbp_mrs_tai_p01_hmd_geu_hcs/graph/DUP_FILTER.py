from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.config.ConfigStore import *
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.udfs.UDFs import *

def DUP_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
