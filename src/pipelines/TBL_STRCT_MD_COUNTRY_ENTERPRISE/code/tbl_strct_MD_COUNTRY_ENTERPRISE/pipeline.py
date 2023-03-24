from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_COUNTRY_ENTERPRISE.config.ConfigStore import *
from tbl_strct_MD_COUNTRY_ENTERPRISE.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_COUNTRY_ENTERPRISE.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_CTRY_CURR = sql_MD_CTRY_CURR(spark)
    MD_CTRY_CURR(spark, df_sql_MD_CTRY_CURR)
    df_sql_MD_CTRY = sql_MD_CTRY(spark)
    df_sql_MD_CTRY_TEXT = sql_MD_CTRY_TEXT(spark)
    MD_CTRY(spark, df_sql_MD_CTRY)
    MD_CTRY_TEXT(spark, df_sql_MD_CTRY_TEXT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_COUNTRY_ENTERPRISE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_COUNTRY_ENTERPRISE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
