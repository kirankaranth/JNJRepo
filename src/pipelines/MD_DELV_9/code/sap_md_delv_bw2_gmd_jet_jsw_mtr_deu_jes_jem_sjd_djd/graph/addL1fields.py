from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.config.ConfigStore import *
from sap_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("_pk_", to_json(expr("named_struct('REF_DOC_NUM', REF_DOC_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('REF_DOC_NUM', REF_DOC_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
