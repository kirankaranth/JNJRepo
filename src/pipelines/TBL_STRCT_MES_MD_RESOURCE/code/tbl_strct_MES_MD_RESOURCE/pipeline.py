from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_RESOURCE.config.ConfigStore import *
from tbl_strct_MES_MD_RESOURCE.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_RESOURCE.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_ETH_LINE = sql_MES_MD_ETH_LINE(spark)
    MES_MD_ETH_LINE(spark, df_sql_MES_MD_ETH_LINE)
    df_sql_MES_MD_ETH_CELL = sql_MES_MD_ETH_CELL(spark)
    df_sql_MES_MD_PRDTN_STS = sql_MES_MD_PRDTN_STS(spark)
    MES_MD_PRDTN_STS(spark, df_sql_MES_MD_PRDTN_STS)
    df_sql_MES_MD_BILL_OF_PRCS_BASE = sql_MES_MD_BILL_OF_PRCS_BASE(spark)
    MES_MD_BILL_OF_PRCS_BASE(spark, df_sql_MES_MD_BILL_OF_PRCS_BASE)
    df_sql_MES_MD_RSRS = sql_MES_MD_RSRS(spark)
    df_sql_MES_MD_ETH_RSRS_TYPE = sql_MES_MD_ETH_RSRS_TYPE(spark)
    MES_MD_ETH_CELL(spark, df_sql_MES_MD_ETH_CELL)
    df_sql_MES_MD_EMP = sql_MES_MD_EMP(spark)
    MES_MD_EMP(spark, df_sql_MES_MD_EMP)
    df_sql_MES_MD_OBJ_TYPE = sql_MES_MD_OBJ_TYPE(spark)
    MES_MD_OBJ_TYPE(spark, df_sql_MES_MD_OBJ_TYPE)
    MES_MD_ETH_RSRS_TYPE(spark, df_sql_MES_MD_ETH_RSRS_TYPE)
    MES_MD_RSRS(spark, df_sql_MES_MD_RSRS)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_RESOURCE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_RESOURCE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
