from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_WORKCENTER.config.ConfigStore import *
from tbl_strct_MD_WORKCENTER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_WORKCENTER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_WRK_TIMES = sql_MD_WRK_TIMES(spark)
    MD_WRK_TIMES(spark, df_sql_MD_WRK_TIMES)
    df_sql_MD_WRK_CTR = sql_MD_WRK_CTR(spark)
    df_sql_MD_PUBLC_HOL_CAL = sql_MD_PUBLC_HOL_CAL(spark)
    MD_PUBLC_HOL_CAL(spark, df_sql_MD_PUBLC_HOL_CAL)
    MD_WRK_CTR(spark, df_sql_MD_WRK_CTR)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_WORKCENTER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_WORKCENTER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
