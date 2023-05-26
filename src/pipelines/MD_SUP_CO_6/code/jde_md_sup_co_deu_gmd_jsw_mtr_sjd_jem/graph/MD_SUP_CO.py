from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem.config.ConfigStore import *
from jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem.udfs.UDFs import *

def MD_SUP_CO(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SUP_CO")
