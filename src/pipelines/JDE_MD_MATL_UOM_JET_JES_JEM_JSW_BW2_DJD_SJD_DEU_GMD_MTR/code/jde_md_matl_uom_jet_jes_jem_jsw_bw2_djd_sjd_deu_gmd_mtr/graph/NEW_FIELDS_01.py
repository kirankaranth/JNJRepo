from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.config.ConfigStore import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.udfs.UDFs import *

def NEW_FIELDS_01(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("UOM_CD", trim(col("DRKY")))\
        .withColumn("DIM_KEY", trim(col("DRSPHD")))\
        .withColumn("UOM_SHRT_TEXT", trim(col("DRDL02")))\
        .withColumn("UOM_LONG_TEXT", trim(col("DRDL01")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'UOM_CD', UOM_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'UOM_CD', UOM_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
