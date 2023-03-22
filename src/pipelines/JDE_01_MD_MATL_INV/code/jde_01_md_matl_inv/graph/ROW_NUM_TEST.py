from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def ROW_NUM_TEST(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT \r\n    * \r\nFROM \r\n    (SELECT \r\n        *,\r\n        row_number() OVER(PARTITION BY SRC_SYS_CD, MATL_NUM, PLNT_CD, SLOC_CD, BTCH_NUM, BTCH_STS_CD \r\n        ORDER BY SRC_SYS_CD, MATL_NUM, PLNT_CD, SLOC_CD, BTCH_NUM, BTCH_STS_CD) rank_no\r\n    FROM\r\n        in0) a\r\nWHERE rank_no = 1"
    )

    return df1
