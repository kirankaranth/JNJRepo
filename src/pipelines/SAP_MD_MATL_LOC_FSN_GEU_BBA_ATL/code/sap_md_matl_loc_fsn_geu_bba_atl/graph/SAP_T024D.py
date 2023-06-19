from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_fsn_geu_bba_atl.config.ConfigStore import *
from sap_md_matl_loc_fsn_geu_bba_atl.udfs.UDFs import *

def SAP_T024D(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.t024d")
