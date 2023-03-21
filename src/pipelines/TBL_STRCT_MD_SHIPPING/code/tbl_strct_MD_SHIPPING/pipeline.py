from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_SHIPPING.config.ConfigStore import *
from tbl_strct_MD_SHIPPING.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_SHIPPING.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SHIPPING_STN_TCKG_NUM = sql_MD_SHIPPING_STN_TCKG_NUM(spark)
    MD_SHIPPING_STN_TCKG_NUM(spark, df_sql_MD_SHIPPING_STN_TCKG_NUM)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_SHIPPING")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_SHIPPING")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
