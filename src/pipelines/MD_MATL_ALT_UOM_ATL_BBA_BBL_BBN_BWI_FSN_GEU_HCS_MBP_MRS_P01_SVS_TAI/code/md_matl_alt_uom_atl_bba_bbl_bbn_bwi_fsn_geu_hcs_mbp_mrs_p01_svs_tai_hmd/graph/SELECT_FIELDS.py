from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *

def SELECT_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("MATNR"), col("MEINS"), col("MANDT"), col("_deleted_"))
