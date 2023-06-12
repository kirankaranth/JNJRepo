from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_crncy_svs_bwi_atl.config.ConfigStore import *
from sap_md_crncy_svs_bwi_atl.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CRNCY_CD", col("WAERS"))\
        .withColumn("ISO_CRNCY_CD", trim(col("ISOCD")))\
        .withColumn("CRNCY_ALT_CD", trim(col("ALTWR")))\
        .withColumn(
          "VLD_TO_DTTM",
          when(
              (
                ((col("GDATU") == lit("00000000")) | (length(regexp_replace(col("GDATU"), "(\\d+)", "")) > lit(0)))
                | (length(col("GDATU")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(col("GDATU"), "yyyyMMdd"))
        )\
        .withColumn("PRMRY_SAP_CRNCY_CD_ISO_CD", trim(col("XPRIMARY")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CRNCY_CD', CRNCY_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CRNCY_CD', CRNCY_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
