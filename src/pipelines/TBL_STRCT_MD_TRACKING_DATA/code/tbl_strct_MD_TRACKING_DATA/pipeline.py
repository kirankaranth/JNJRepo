from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_TRACKING_DATA.config.ConfigStore import *
from tbl_strct_MD_TRACKING_DATA.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_TRACKING_DATA.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_TCKG_DATA_HDR = sql_MD_TCKG_DATA_HDR(spark)
    MD_TCKG_DATA_HDR(spark, df_sql_MD_TCKG_DATA_HDR)
    df_sql_MD_TCKG_DATA_ITM = sql_MD_TCKG_DATA_ITM(spark)
    MD_TCKG_DATA_ITM(spark, df_sql_MD_TCKG_DATA_ITM)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_TRACKING_DATA")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_TRACKING_DATA")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
