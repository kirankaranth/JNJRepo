from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_SITE_PLANT.config.ConfigStore import *
from tbl_strct_MES_MD_SITE_PLANT.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_SITE_PLANT.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_ORG = sql_MES_MD_ORG(spark)
    df_sql_MES_MD_FCTRY = sql_MES_MD_FCTRY(spark)
    df_sql_MES_MD_ENTRP = sql_MES_MD_ENTRP(spark)
    MES_MD_ORG(spark, df_sql_MES_MD_ORG)
    df_sql_MES_MD_LOC = sql_MES_MD_LOC(spark)
    MES_MD_FCTRY(spark, df_sql_MES_MD_FCTRY)
    MES_MD_LOC(spark, df_sql_MES_MD_LOC)
    MES_MD_ENTRP(spark, df_sql_MES_MD_ENTRP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_SITE_PLANT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_SITE_PLANT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
