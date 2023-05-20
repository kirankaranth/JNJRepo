from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.config.ConfigStore import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("imlitm"))\
        .withColumn(
          "SL_ORG_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "DSTR_CHNL_CD",
          lit(
            "#"
          )
        )\
        .withColumn("PROD_HIER_CD", trim(col("imsrp1")))\
        .withColumn("MATL_GRP_2", trim(col("imprp4")))\
        .withColumn("MATL_GRP_3", trim(col("imprp3")))\
        .withColumn("MATL_GRP_4", trim(col("imsrp4")))\
        .withColumn("MATL_GRP_5", trim(col("imprp5")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'SL_ORG_NUM', SL_ORG_NUM, 'DSTR_CHNL_CD', DSTR_CHNL_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'SL_ORG_NUM', SL_ORG_NUM, 'DSTR_CHNL_CD', DSTR_CHNL_CD)"
              )
            )
          )
        )\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
