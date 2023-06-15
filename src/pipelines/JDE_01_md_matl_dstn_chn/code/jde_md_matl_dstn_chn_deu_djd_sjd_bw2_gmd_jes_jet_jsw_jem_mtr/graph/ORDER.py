from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.config.ConfigStore import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.udfs.UDFs import *

def ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("SL_ORG_NUM"), 
        col("DSTR_CHNL_CD"), 
        col("PROD_HIER_CD"), 
        col("MATL_GRP_2"), 
        col("MATL_GRP_3"), 
        col("MATL_GRP_4"), 
        col("MATL_GRP_5"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_pk_md5_"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_deleted_")
    )
