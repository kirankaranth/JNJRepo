from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_PURCHASE_ORDER.config.ConfigStore import *
from tbl_strct_MD_PURCHASE_ORDER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_PURCHASE_ORDER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PO_LINE = sql_MD_PO_LINE(spark)
    MD_PO_LINE(spark, df_sql_MD_PO_LINE)
    df_sql_MD_PRCH_DELV_CNFRMS = sql_MD_PRCH_DELV_CNFRMS(spark)
    MD_PRCH_DELV_CNFRMS(spark, df_sql_MD_PRCH_DELV_CNFRMS)
    df_sql_MD_PO_SCHED_LINE_DELV = sql_MD_PO_SCHED_LINE_DELV(spark)
    MD_PO_SCHED_LINE_DELV(spark, df_sql_MD_PO_SCHED_LINE_DELV)
    df_sql_MD_PO = sql_MD_PO(spark)
    MD_PO(spark, df_sql_MD_PO)
    df_sql_MD_PO_HIST = sql_MD_PO_HIST(spark)
    MD_PO_HIST(spark, df_sql_MD_PO_HIST)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_PURCHASE_ORDER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_PURCHASE_ORDER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
