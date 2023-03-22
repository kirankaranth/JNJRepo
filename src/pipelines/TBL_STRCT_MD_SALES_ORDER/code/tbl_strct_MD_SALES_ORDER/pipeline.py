from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_SALES_ORDER.config.ConfigStore import *
from tbl_strct_MD_SALES_ORDER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_SALES_ORDER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SLS_ORDR = sql_MD_SLS_ORDR(spark)
    MD_SLS_ORDR(spark, df_sql_MD_SLS_ORDR)
    df_sql_MD_SLS_DOC_PTNR_FUNC = sql_MD_SLS_DOC_PTNR_FUNC(spark)
    df_sql_MD_ORDR_CNFRM = sql_MD_ORDR_CNFRM(spark)
    MD_ORDR_CNFRM(spark, df_sql_MD_ORDR_CNFRM)
    MD_SLS_DOC_PTNR_FUNC(spark, df_sql_MD_SLS_DOC_PTNR_FUNC)
    df_sql_MD_SLS_ORDR_LINE = sql_MD_SLS_ORDR_LINE(spark)
    df_sql_MD_SLS_ORDR_SCHED_LINE_DELV = sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark)
    MD_SLS_ORDR_SCHED_LINE_DELV(spark, df_sql_MD_SLS_ORDR_SCHED_LINE_DELV)
    df_sql_MD_SLS_RQR_INDIV_REC = sql_MD_SLS_RQR_INDIV_REC(spark)
    MD_SLS_RQR_INDIV_REC(spark, df_sql_MD_SLS_RQR_INDIV_REC)
    MD_SLS_ORDR_LINE(spark, df_sql_MD_SLS_ORDR_LINE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_SALES_ORDER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_SALES_ORDER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
