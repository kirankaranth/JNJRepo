from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def MD_MATL_UOM(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_UOM")
