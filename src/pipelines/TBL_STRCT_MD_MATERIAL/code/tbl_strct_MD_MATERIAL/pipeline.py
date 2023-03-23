from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_MATERIAL.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_MATL_UOM = sql_MD_MATL_UOM(spark)
    df_sql_MD_MATL_ALT_UOM = sql_MD_MATL_ALT_UOM(spark)
    df_sql_MD_MATL_DSTN_CHN = sql_MD_MATL_DSTN_CHN(spark)
    MD_MATL_DSTN_CHN(spark, df_sql_MD_MATL_DSTN_CHN)
    df_sql_MD_HIERARCHY_SETS = sql_MD_HIERARCHY_SETS(spark)
    MD_HIERARCHY_SETS(spark, df_sql_MD_HIERARCHY_SETS)
    df_sql_MD_MATL_MFG_ALLC = sql_MD_MATL_MFG_ALLC(spark)
    MD_MATL_MFG_ALLC(spark, df_sql_MD_MATL_MFG_ALLC)
    df_sql_MD_MATL_DOC_STK_CHG_DATA = sql_MD_MATL_DOC_STK_CHG_DATA(spark)
    MD_MATL_DOC_STK_CHG_DATA(spark, df_sql_MD_MATL_DOC_STK_CHG_DATA)
    df_sql_MD_MATL_MFG_RTG = sql_MD_MATL_MFG_RTG(spark)
    df_sql_MD_MATL_LOC_QUAL = sql_MD_MATL_LOC_QUAL(spark)
    MD_MATL_LOC_QUAL(spark, df_sql_MD_MATL_LOC_QUAL)
    MD_MATL_UOM(spark, df_sql_MD_MATL_UOM)
    df_sql_MD_MATL = sql_MD_MATL(spark)
    MD_MATL(spark, df_sql_MD_MATL)
    df_sql_MD_MATL_DESC = sql_MD_MATL_DESC(spark)
    df_sql_MD_MATL_LOC = sql_MD_MATL_LOC(spark)
    MD_MATL_MFG_RTG(spark, df_sql_MD_MATL_MFG_RTG)
    MD_MATL_ALT_UOM(spark, df_sql_MD_MATL_ALT_UOM)
    MD_MATL_DESC(spark, df_sql_MD_MATL_DESC)
    MD_MATL_LOC(spark, df_sql_MD_MATL_LOC)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_MATERIAL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_MATERIAL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
