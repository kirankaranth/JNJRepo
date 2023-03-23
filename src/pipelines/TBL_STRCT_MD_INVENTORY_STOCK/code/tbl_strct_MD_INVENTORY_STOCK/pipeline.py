from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_INVENTORY_STOCK.config.ConfigStore import *
from tbl_strct_MD_INVENTORY_STOCK.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_INVENTORY_STOCK.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_MATL_VALUT_HIST = sql_MD_MATL_VALUT_HIST(spark)
    df_sql_MD_MATL_INV_HIST = sql_MD_MATL_INV_HIST(spark)
    MD_MATL_INV_HIST(spark, df_sql_MD_MATL_INV_HIST)
    df_sql_MD_MATL_MVMT_ITM = sql_MD_MATL_MVMT_ITM(spark)
    df_sql_MD_MATL_INV = sql_MD_MATL_INV(spark)
    df_sql_MD_MATL_MVMT = sql_MD_MATL_MVMT(spark)
    df_sql_MD_MATL_MVMT_HDR = sql_MD_MATL_MVMT_HDR(spark)
    MD_MATL_MVMT_HDR(spark, df_sql_MD_MATL_MVMT_HDR)
    df_sql_MD_MATL_VALUT = sql_MD_MATL_VALUT(spark)
    MD_MATL_MVMT(spark, df_sql_MD_MATL_MVMT)
    df_sql_MD_MATL_INV_SLS_HIST = sql_MD_MATL_INV_SLS_HIST(spark)
    MD_MATL_INV_SLS_HIST(spark, df_sql_MD_MATL_INV_SLS_HIST)
    df_sql_MD_MATL_INV_SLS = sql_MD_MATL_INV_SLS(spark)
    MD_MATL_INV_SLS(spark, df_sql_MD_MATL_INV_SLS)
    MD_MATL_INV(spark, df_sql_MD_MATL_INV)
    MD_MATL_VALUT(spark, df_sql_MD_MATL_VALUT)
    MD_MATL_MVMT_ITM(spark, df_sql_MD_MATL_MVMT_ITM)
    MD_MATL_VALUT_HIST(spark, df_sql_MD_MATL_VALUT_HIST)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_INVENTORY_STOCK")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_INVENTORY_STOCK")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
