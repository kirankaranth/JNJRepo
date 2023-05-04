from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bill_doc_hdr_hmd.config.ConfigStore import *
from sap_md_bill_doc_hdr_hmd.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("BILL_DOC"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
