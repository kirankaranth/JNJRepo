from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_WORKFLOW.config.ConfigStore import *
from tbl_strct_MES_MD_WORKFLOW.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_WORKFLOW.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_RTE_STEP = sql_MES_MD_RTE_STEP(spark)
    df_sql_MES_MD_WRKF_BASE = sql_MES_MD_WRKF_BASE(spark)
    MES_MD_WRKF_BASE(spark, df_sql_MES_MD_WRKF_BASE)
    df_sql_MES_MD_RPT_EXTRUSION_OPR = sql_MES_MD_RPT_EXTRUSION_OPR(spark)
    df_sql_MES_MD_WRKF_STEP = sql_MES_MD_WRKF_STEP(spark)
    MES_MD_WRKF_STEP(spark, df_sql_MES_MD_WRKF_STEP)
    df_sql_MES_MD_OUTB_CIO_RQST = sql_MES_MD_OUTB_CIO_RQST(spark)
    MES_MD_OUTB_CIO_RQST(spark, df_sql_MES_MD_OUTB_CIO_RQST)
    df_sql_MES_MD_WRKF = sql_MES_MD_WRKF(spark)
    MES_MD_WRKF(spark, df_sql_MES_MD_WRKF)
    df_sql_MES_MD_OPR = sql_MES_MD_OPR(spark)
    MES_MD_OPR(spark, df_sql_MES_MD_OPR)
    MES_MD_RPT_EXTRUSION_OPR(spark, df_sql_MES_MD_RPT_EXTRUSION_OPR)
    MES_MD_RTE_STEP(spark, df_sql_MES_MD_RTE_STEP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_WORKFLOW")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_WORKFLOW")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
