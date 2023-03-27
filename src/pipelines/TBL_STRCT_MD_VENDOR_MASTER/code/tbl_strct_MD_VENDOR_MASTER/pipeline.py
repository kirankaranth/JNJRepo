from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_VENDOR_MASTER.config.ConfigStore import *
from tbl_strct_MD_VENDOR_MASTER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_VENDOR_MASTER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP_PRCHSNG_ORG = sql_MD_SUP_PRCHSNG_ORG(spark)
    df_sql_MD_SUP_CO = sql_MD_SUP_CO(spark)
    MD_SUP_CO(spark, df_sql_MD_SUP_CO)
    df_sql_MD_SUP_PTNR_FUNC = sql_MD_SUP_PTNR_FUNC(spark)
    MD_SUP_PTNR_FUNC(spark, df_sql_MD_SUP_PTNR_FUNC)
    df_sql_MD_SUP = sql_MD_SUP(spark)
    MD_SUP(spark, df_sql_MD_SUP)
    MD_SUP_PRCHSNG_ORG(spark, df_sql_MD_SUP_PRCHSNG_ORG)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_VENDOR_MASTER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_VENDOR_MASTER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
