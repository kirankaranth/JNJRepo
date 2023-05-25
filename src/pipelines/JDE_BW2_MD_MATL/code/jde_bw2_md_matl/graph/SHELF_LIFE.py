from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *

def SHELF_LIFE(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    df1 = spark.sql(
        "SELECT IVLITM, min_shelf_life_in_days FROM\r\n\t(\r\n\t\tSELECT *, ROW_NUMBER () OVER (PARTITION BY IVLITM ORDER BY min_shelf_life_in_days DESC) AS RNK FROM (\r\n\t\tSELECT IVLITM,\r\n\t\t\tCAST(CASE \r\n\t\t\t\tWHEN IVDSC1 IS NOT NULL THEN IVDSC1 * 30\r\n\t\t\t\tELSE\r\n\t\t\t\t\t(SELECT in1.GPURAB * 30\r\n\t\t\t\t\tFROM in1\r\n\t\t\t\t\tWHERE TRIM(in1.GPGS1A) = 'R5841110'\r\n\t\t\t\t\tAND TRIM(in1.GPGS2A) = 'MinShelfLife'\r\n\t\t\t\t\tAND in1._deleted_ = 'F')\r\n\t\t\t\tEND AS DECIMAL(18,4)) AS min_shelf_life_in_days\r\n\t\tFROM in0 \r\n\t\tWHERE in0._deleted_ = 'F'\r\n\t\t\tAND in0.IVXRT='XL' ))\r\n\t\t\tWHERE RNK = 1"
    )

    return df1
