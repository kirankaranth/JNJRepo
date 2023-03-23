from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_RESERVATIONS.config.ConfigStore import *
from tbl_strct_MD_RESERVATIONS.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_RESERVATIONS.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_LNSET_RESV = sql_MD_LNSET_RESV(spark)
    MD_LNSET_RESV(spark, df_sql_MD_LNSET_RESV)
    df_sql_MD_RESV_HIST_LOG_TBL = sql_MD_RESV_HIST_LOG_TBL(spark)
    MD_RESV_HIST_LOG_TBL(spark, df_sql_MD_RESV_HIST_LOG_TBL)
    df_sql_MD_LNSET_RESV_POS = sql_MD_LNSET_RESV_POS(spark)
    MD_LNSET_RESV_POS(spark, df_sql_MD_LNSET_RESV_POS)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_RESERVATIONS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_RESERVATIONS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
