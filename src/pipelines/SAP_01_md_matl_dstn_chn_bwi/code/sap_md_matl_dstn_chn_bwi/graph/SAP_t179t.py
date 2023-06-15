from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_dstn_chn_bwi.config.ConfigStore import *
from sap_md_matl_dstn_chn_bwi.udfs.UDFs import *

def SAP_t179t(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.t179t")
