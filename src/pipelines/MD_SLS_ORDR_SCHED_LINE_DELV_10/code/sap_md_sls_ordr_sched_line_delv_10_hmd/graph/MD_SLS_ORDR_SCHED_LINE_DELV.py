from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_ordr_sched_line_delv_10_hmd.config.ConfigStore import *
from sap_md_sls_ordr_sched_line_delv_10_hmd.udfs.UDFs import *

def MD_SLS_ORDR_SCHED_LINE_DELV(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SLS_ORDR_SCHED_LINE_DELV")
