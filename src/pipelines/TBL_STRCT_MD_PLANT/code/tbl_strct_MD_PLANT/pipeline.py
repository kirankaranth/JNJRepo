from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_PLANT.config.ConfigStore import *
from tbl_strct_MD_PLANT.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_PLANT.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PLNT = sql_MD_PLNT(spark)
    MD_PLNT(spark, df_sql_MD_PLNT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_PLANT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_PLANT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
