from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def CHARACTERISTICS(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    in2.createOrReplaceTempView("in2")
    df1 = spark.sql(
        "SELECT\r\n    'TYPE_OF_MATERIAL' AS cabn_filter,\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'TYPE_OF_MATERIAL'\r\nUNION\r\nSELECT\r\n    'STERILE',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'STERILE'\r\nUNION\r\nSELECT\r\n    'BRAVO_MINOR_CODE',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'BRAVO_MINOR_CODE'\r\nUNION\r\nSELECT\r\n    'NDL_SLS_TYPE',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'NDL_SLS_TYPE'\r\nUNION\r\nSELECT\r\n    'SUTURE_LENGTH_INCH',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'SUTURE_LENGTH_INCH'\r\nUNION\r\nSELECT\r\n    'NDL_COLOR',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'NDL_COLOR'\r\nUNION\r\nSELECT\r\n    'NDL_ALLOY',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'NDL_ALLOY'\r\nUNION\r\nSELECT\r\n    'SUTURE_USP',\r\n    in0.OBJEK,\r\n    in0.ATWRT,\r\n    in1.OBJEK MAT_NUM\r\nFROM\r\n    in0\r\n        INNER JOIN\r\n    in1 ON in0.OBJEK = in1.CUOBJ\r\n        INNER JOIN\r\n    in2 ON in0.ATINN = in2.ATINN AND in2.ATNAM = 'SUTURE_USP'"
    )

    return df1
