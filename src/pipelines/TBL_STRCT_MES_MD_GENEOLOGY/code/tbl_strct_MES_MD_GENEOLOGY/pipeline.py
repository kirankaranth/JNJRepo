from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_GENEOLOGY.config.ConfigStore import *
from tbl_strct_MES_MD_GENEOLOGY.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_GENEOLOGY.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_TASKLIST_BASE = sql_MES_MD_TASKLIST_BASE(spark)
    df_sql_MES_MD_DATA_CLCT_DEF = sql_MES_MD_DATA_CLCT_DEF(spark)
    MES_MD_DATA_CLCT_DEF(spark, df_sql_MES_MD_DATA_CLCT_DEF)
    MES_MD_TASKLIST_BASE(spark, df_sql_MES_MD_TASKLIST_BASE)
    df_sql_MES_MD_SPEC_BASE = sql_MES_MD_SPEC_BASE(spark)
    MES_MD_SPEC_BASE(spark, df_sql_MES_MD_SPEC_BASE)
    df_sql_MES_MD_SPEC = sql_MES_MD_SPEC(spark)
    MES_MD_SPEC(spark, df_sql_MES_MD_SPEC)
    df_sql_MES_MD_RECIPE_SPEC_ITM = sql_MES_MD_RECIPE_SPEC_ITM(spark)
    df_sql_MES_MD_TASK_ITM = sql_MES_MD_TASK_ITM(spark)
    df_sql_MES_MD_ELCTRNC_PCDR_BASE = sql_MES_MD_ELCTRNC_PCDR_BASE(spark)
    df_sql_MES_MD_MFG_ORDR_MATL_LIST = sql_MES_MD_MFG_ORDR_MATL_LIST(spark)
    MES_MD_MFG_ORDR_MATL_LIST(spark, df_sql_MES_MD_MFG_ORDR_MATL_LIST)
    df_sql_MES_MD_DATA_PT_HIST_DTL = sql_MES_MD_DATA_PT_HIST_DTL(spark)
    MES_MD_ELCTRNC_PCDR_BASE(spark, df_sql_MES_MD_ELCTRNC_PCDR_BASE)
    df_sql_MES_MD_ELCTRNC_PCDR = sql_MES_MD_ELCTRNC_PCDR(spark)
    MES_MD_TASK_ITM(spark, df_sql_MES_MD_TASK_ITM)
    MES_MD_DATA_PT_HIST_DTL(spark, df_sql_MES_MD_DATA_PT_HIST_DTL)
    df_sql_MES_MD_DATA_CLCT_DEF_BASE = sql_MES_MD_DATA_CLCT_DEF_BASE(spark)
    MES_MD_DATA_CLCT_DEF_BASE(spark, df_sql_MES_MD_DATA_CLCT_DEF_BASE)
    MES_MD_ELCTRNC_PCDR(spark, df_sql_MES_MD_ELCTRNC_PCDR)
    MES_MD_RECIPE_SPEC_ITM(spark, df_sql_MES_MD_RECIPE_SPEC_ITM)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_GENEOLOGY")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_GENEOLOGY")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
