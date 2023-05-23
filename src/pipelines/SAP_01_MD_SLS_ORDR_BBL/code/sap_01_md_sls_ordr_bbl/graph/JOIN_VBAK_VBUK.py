from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bbl.config.ConfigStore import *
from sap_01_md_sls_ordr_bbl.udfs.UDFs import *

def JOIN_VBAK_VBUK(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.SLS_ORDR_DOC_ID") == col("in1.SLS_ORDR_DOC_ID_VBUK")), "left_outer")
