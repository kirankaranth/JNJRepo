from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn("PLNT_NM", trim(col("NAME1")))\
        .withColumn("CTRY_CD", trim(col("LAND1")))\
        .withColumn("PLNT_CAT_CD", trim(col("NODETYPE")))\
        .withColumn("RGN_CD", trim(col("REGIO")))\
        .withColumn("VALUT_AREA", trim(col("BWKEY")))\
        .withColumn("CO_CD", trim(col("BUKRS")))\
        .withColumn("CAL_NUM", trim(col("FABKL")))\
        .withColumn("CTRY_SHRT_NM", trim(lookup("LU_SAP_T005T", col("LAND1")).getField("LANDX")))\
        .withColumn("ADDR_LINE_1_TXT", trim(col("STRAS")))\
        .withColumn("PSTL_CD_NUM", trim(col("PSTLZ")))\
        .withColumn("CITY_NM", trim(col("ORT01")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PLNT_CD', PLNT_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PLNT_CD', PLNT_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("CRNCY_KEY   ", trim(col("WAERS")))
