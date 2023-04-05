from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", coalesce(col("IMLITM"), col("UMITM")))\
        .withColumn("ALT_UOM_CD", col("UMUM"))\
        .withColumn("BASE_UOM_CD", col("UMRUM"))\
        .withColumn("STD_UOM_CD", col("IMUOM1"))\
        .withColumn("FACT_NUMRTR_MEAS", (col("UMCONV") / lit(10000000)).cast(DecimalType(18, 4)))\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("_l0_upt_", col("f41002_upt_"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'ALT_UOM_CD', ALT_UOM_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'ALT_UOM_CD', ALT_UOM_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())
