from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PR = sql_MD_PR(spark)
    MD_PR(spark, df_sql_MD_PR)
    df_sql_MD_PR_LINE = sql_MD_PR_LINE(spark)
    MD_PR_LINE(spark, df_sql_MD_PR_LINE)
    df_sql_MD_PRCH_INFO_ORG = sql_MD_PRCH_INFO_ORG(spark)
    MD_PRCH_INFO_ORG(spark, df_sql_MD_PRCH_INFO_ORG)
    df_sql_MD_STRG_BINS = sql_MD_STRG_BINS(spark)
    MD_STRG_BINS(spark, df_sql_MD_STRG_BINS)
    df_sql_MD_PRCHSNG_ORG = sql_MD_PRCHSNG_ORG(spark)
    MD_PRCHSNG_ORG(spark, df_sql_MD_PRCHSNG_ORG)
    df_sql_MD_PRCH_INFO = sql_MD_PRCH_INFO(spark)
    MD_PRCH_INFO(spark, df_sql_MD_PRCH_INFO)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
