from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bom.config.ConfigStore import *
from sap_md_matl_bom.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn("BOM_USG_CD", col("STLAN"))\
        .withColumn("BOM_NUM", col("STLNR"))\
        .withColumn("ALT_BOM_NUM", col("STLAL"))\
        .withColumn(
          "CRT_DTTM",
          when((col("ANDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ANDAT"), "yyyyMMdd"))
        )\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn("FROM_LOT_SIZE_QTY", trim(col("LOSVN")).cast(DecimalType(18, 4)))\
        .withColumn("TO_LOT_SIZE_QTY", trim(col("LOSBS")).cast(DecimalType(18, 4)))\
        .withColumn("USER_WHO_CRT_REC", trim(col("ANNAM")))\
        .withColumn("NM_OF_PRSN_WHO_CHG_OBJ", trim(col("AENAM")))\
        .withColumn("CNFG_MATL_IN", trim(col("CSLTY")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD, 'BOM_USG_CD', BOM_USG_CD, 'BOM_NUM', BOM_NUM, 'ALT_BOM_NUM', ALT_BOM_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD, 'BOM_USG_CD', BOM_USG_CD, 'BOM_NUM', BOM_NUM, 'ALT_BOM_NUM', ALT_BOM_NUM)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
