from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_inv_p01_svs.config.ConfigStore import *
from sap_md_matl_inv_p01_svs.udfs.UDFs import *

def MD_MATL_INV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_INV")
