from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.config.ConfigStore import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.udfs.UDFs import *

def MD_MATL_INV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_INV")
