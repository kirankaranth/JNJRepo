from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MES_MD_MATERIAL.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_MATERIAL.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_PROD_RVSN_BASE = sql_MES_MD_PROD_RVSN_BASE(spark)
    MES_MD_PROD_RVSN_BASE(spark, df_sql_MES_MD_PROD_RVSN_BASE)
    df_sql_MES_MD_PROD_RVSN = sql_MES_MD_PROD_RVSN(spark)
    MES_MD_PROD_RVSN(spark, df_sql_MES_MD_PROD_RVSN)
    df_sql_MES_MD_PROD_BASE = sql_MES_MD_PROD_BASE(spark)
    MES_MD_PROD_BASE(spark, df_sql_MES_MD_PROD_BASE)
    df_sql_MES_MD_PROD = sql_MES_MD_PROD(spark)
    MES_MD_PROD(spark, df_sql_MES_MD_PROD)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_MATERIAL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_MATERIAL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
