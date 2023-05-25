from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.config.ConfigStore import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.udfs.UDFs import *

def Reformat_MARA(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("MATNR").alias("MATNR_MARA"), col("MEINS"))
