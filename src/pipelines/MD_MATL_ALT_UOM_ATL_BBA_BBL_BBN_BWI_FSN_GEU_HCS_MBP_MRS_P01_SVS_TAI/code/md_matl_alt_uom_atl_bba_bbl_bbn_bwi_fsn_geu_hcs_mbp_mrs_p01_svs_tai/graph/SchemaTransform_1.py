from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("ALT_UOM_CD", col("MEINH"))\
        .withColumn("BASE_UOM_CD", trim(lookup("LU_MARA_MEINS", col("MATNR")).getField("MEINS")))\
        .withColumn("LCL_FACT_NUMRTR_MEAS", col("UMREZ").cast(DecimalType(18, 4)))\
        .withColumn("LCL_FACT_DENOM_MEAS", col("UMREN").cast(DecimalType(18, 4)))\
        .withColumn("FACT_NUMRTR_MEAS", col("UMREZ").cast(DecimalType(18, 4)))\
        .withColumn("FACT_DENOM_MEAS", col("UMREN").cast(DecimalType(18, 4)))\
        .withColumn("GTIN_NUM", trim(col("EAN11")))\
        .withColumn("GTIN_CAT_CD", trim(col("NUMTP")))\
        .withColumn("WDTH_MEAS", col("BREIT").cast(DecimalType(18, 4)))\
        .withColumn("DIM_UOM_CD", trim(col("MEABM")))\
        .withColumn("GRS_WT_MEAS", col("BRGEW").cast(DecimalType(18, 4)))\
        .withColumn("WT_UOM_CD", trim(col("GEWEI")))\
        .withColumn("LGTH_MEAS", col("LAENG").cast(DecimalType(18, 4)))\
        .withColumn("HGHT_MEAS", col("HOEHE").cast(DecimalType(18, 4)))\
        .withColumn("VOL_MEAS", col("VOLUM").cast(DecimalType(18, 4)))\
        .withColumn(
          "GTIN_NUM_HRMZD",
          when((length(expr("replace(trim(EAN11), ' ', '')")) == lit(0)), lit(""))\
            .otherwise(concat(
            expr("substring('00000000000000', 1, (14 - length(replace(trim(EAN11), ' ', ''))))"), 
            expr("replace(trim(EAN11), ' ', '')")
          ))
        )\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'ALT_UOM_CD', ALT_UOM_CD, 'BASE_UOM_CD', BASE_UOM_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'ALT_UOM_CD', ALT_UOM_CD, 'BASE_UOM_CD', BASE_UOM_CD)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
