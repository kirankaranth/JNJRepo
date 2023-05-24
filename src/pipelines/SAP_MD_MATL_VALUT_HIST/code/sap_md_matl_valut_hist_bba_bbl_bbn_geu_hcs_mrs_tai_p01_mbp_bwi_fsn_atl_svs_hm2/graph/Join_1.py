from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.config.ConfigStore import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.MATNR") == col("in1.MATNR_MARA")), "left_outer")\
        .select(col("in0.MANDT").alias("MANDT"), col("in1.MEINS").alias("MEINS"), col("in0.MATNR").alias("MATNR"), col("in0.BWKEY").alias("BWKEY"), col("in0.BWTAR").alias("BWTAR"), col("in0.LFGJA").alias("LFGJA"), col("in0.LFMON").alias("LFMON"), col("in0.LBKUM").alias("LBKUM"), col("in0.SALK3").alias("SALK3"), col("in0.VPRSV").alias("VPRSV"), col("in0.VERPR").alias("VERPR"), col("in0.STPRS").alias("STPRS"), col("in0.PEINH").alias("PEINH"), col("in0.BKLAS").alias("BKLAS"), col("in0.SALKV").alias("SALKV"), col("in0.VKSAL").alias("VKSAL"), col("in0._upt_").alias("_upt_"), col("in0._deleted_").alias("_deleted_"))
