from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_cust_mstr_unld_data.config.ConfigStore import *
from sap_md_cust_mstr_unld_data.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("CUST_NUM"), col("UNLOADING_PT"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
