from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_mtr_deu_jem_jsw_sjd.config.ConfigStore import *
from jde_md_sup_mtr_deu_jem_jsw_sjd.udfs.UDFs import *

def MD_SUP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SUP")
