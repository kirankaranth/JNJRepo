from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_mvmt_hdr.config.ConfigStore import *
from sap_md_matl_mvmt_hdr.udfs.UDFs import *

def DS_SAP_MKPF(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mkpf")
