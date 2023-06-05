from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.config.ConfigStore import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PLNT_CD", col("MCMCU"))\
        .withColumn("PLNT_NM", trim(col("MCDC")))\
        .withColumn("CTRY_CD", trim(col("ALCTR")))\
        .withColumn("PLNT_CAT_CD", trim(col("MCSTYL")))\
        .withColumn("RGN_CD", trim(col("MCRP02")))\
        .withColumn("CO_CD", trim(col("MCCO")))\
        .withColumn("PSTL_CD_NUM", trim(col("ALADDZ")))\
        .withColumn("CITY_NM", trim(col("ALCTY1")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PLNT_CD', PLNT_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PLNT_CD', PLNT_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
