from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.config.ConfigStore import *
from jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CRNCY_CD', CRNCY_CD, 'LANG_CD', LANG_CD)")))\
        .withColumn(
          "_pk_md5_",
          md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CRNCY_CD', CRNCY_CD, 'LANG_CD', LANG_CD)")))
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))\
        .withColumn("_l0_upt_", col("_upt_"))
