from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_CURRENCY_ENTERPRISE.config.ConfigStore import *
from tbl_strct_MD_CURRENCY_ENTERPRISE.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_CURRENCY_ENTERPRISE.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_CRNCY_TYPE = sql_MD_CRNCY_TYPE(spark)
    df_sql_MD_CRNCY = sql_MD_CRNCY(spark)
    MD_CRNCY(spark, df_sql_MD_CRNCY)
    df_sql_MD_CRNCY_TEXT = sql_MD_CRNCY_TEXT(spark)
    df_sql_MD_CRNCY_HARMO_VIEW = sql_MD_CRNCY_HARMO_VIEW(spark)
    MD_CRNCY_TEXT(spark, df_sql_MD_CRNCY_TEXT)
    MD_CRNCY_TYPE(spark, df_sql_MD_CRNCY_TYPE)
    MD_CRNCY_HARMO_VIEW(spark, df_sql_MD_CRNCY_HARMO_VIEW)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_CURRENCY_ENTERPRISE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_CURRENCY_ENTERPRISE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
