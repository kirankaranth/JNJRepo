from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_tai.config.ConfigStore import *
from sap_01_md_sls_ordr_tai.udfs.UDFs import *

def GET_DUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("_pk_"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
