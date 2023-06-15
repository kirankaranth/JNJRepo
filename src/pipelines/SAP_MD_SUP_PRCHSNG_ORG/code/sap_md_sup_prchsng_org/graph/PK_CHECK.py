from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_prchsng_org.config.ConfigStore import *
from sap_md_sup_prchsng_org.udfs.UDFs import *

def PK_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("SUP_NUM"), col("PRCHSNG_ORG_NUM"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
