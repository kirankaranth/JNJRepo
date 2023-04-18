from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.udfs.UDFs import *

def CHARACTERISTICS(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    in2.createOrReplaceTempView("in2")
    df1 = spark.sql(
        "SELECT\r\n    'MATERIAL_SPEC' AS cabn_filter,\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'MATERIAL_SPEC'\r\nUNION\r\nSELECT\r\n    'SPEC_REV_LEVEL',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'SPEC_REV_LEVEL'\r\n"
    )

    return df1
