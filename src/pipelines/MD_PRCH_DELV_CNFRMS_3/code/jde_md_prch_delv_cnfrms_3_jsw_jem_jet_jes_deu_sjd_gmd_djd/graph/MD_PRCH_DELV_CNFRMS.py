from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd.config.ConfigStore import *
from jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd.udfs.UDFs import *

def MD_PRCH_DELV_CNFRMS(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_PRCH_DELV_CNFRMS")
