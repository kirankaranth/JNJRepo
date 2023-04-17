from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_BILLING.config.ConfigStore import *
from tbl_strct_MD_BILLING.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_BILLING.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BILL_DOC_HDR = sql_MD_BILL_DOC_HDR(spark)
    MD_BILL_DOC_HDR(spark, df_sql_MD_BILL_DOC_HDR)
    df_sql_MD_BILL_DOC_ITM = sql_MD_BILL_DOC_ITM(spark)
    MD_BILL_DOC_ITM(spark, df_sql_MD_BILL_DOC_ITM)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_BILLING")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_BILLING")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
