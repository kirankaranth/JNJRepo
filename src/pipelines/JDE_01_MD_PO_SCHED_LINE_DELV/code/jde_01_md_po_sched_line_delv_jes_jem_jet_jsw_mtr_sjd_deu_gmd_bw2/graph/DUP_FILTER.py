from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.config.ConfigStore import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.udfs.UDFs import *

def DUP_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
